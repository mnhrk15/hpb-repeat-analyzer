#!/usr/bin/env python3
"""
美容室顧客データリピート分析システム - メインアプリケーション
"""

import os
import pickle
import uuid
from flask import Flask, render_template, request, jsonify, send_file, session, redirect, url_for
from werkzeug.utils import secure_filename
import pandas as pd
import json
from datetime import datetime, timedelta
import traceback
import logging

# カスタムモジュールのインポート
from modules.data_processor import DataProcessor
from modules.repeat_analyzer import RepeatAnalyzer
from modules.visualization import DashboardVisualizer
from modules.report_generator import ReportGenerator

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'dev_secret_key_for_local_flask_run')
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_DATA_FOLDER'] = 'processed_data'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size

# ログ設定
logging.basicConfig(level=logging.INFO)

# アップロードフォルダと処理済みデータフォルダを作成
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_DATA_FOLDER'], exist_ok=True)

def cleanup_session_files():
    """セッションに関連する一時ファイルを削除する"""
    paths_to_clean = ['processed_data_path', 'analysis_results_path']
    for key in paths_to_clean:
        file_path = session.get(key)
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                app.logger.info(f"古い一時ファイルを削除しました: {file_path} (key: {key})")
            except OSError as e:
                app.logger.error(f"一時ファイルの削除に失敗しました: {file_path},エラー: {e}")

@app.route('/')
def index():
    """メインページ - ファイルアップロードと設定画面"""
    cleanup_session_files()
    session.pop('processed_data_path', None)
    session.pop('analysis_results_path', None)
    session.pop('min_date', None)
    session.pop('max_date', None)
    session.pop('analysis_parameters', None)
    session.pop('analysis_performed', None)
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """CSVファイルのアップロード処理"""
    try:
        # 古い一時ファイルがあれば削除 (念のため)
        cleanup_session_files()

        uploaded_files = request.files.getlist('csv_files')
        if not uploaded_files or not uploaded_files[0].filename:
            return jsonify({'error': 'ファイルが選択されていません'}), 400
        
        file_paths = []
        for file_in in uploaded_files: # file変数名がsend_fileと衝突するため変更
            if file_in and file_in.filename.endswith('.csv'):
                filename = secure_filename(file_in.filename)
                # オリジナルのアップロードファイルはセッション間で共有される可能性があるため、
                # UPLOAD_FOLDERもセッションごとに管理するか、毎回上書きを許容する設計とする。
                # ここでは後者のシンプルなアプローチを採用。
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file_in.save(file_path)
                file_paths.append(file_path)
        
        if not file_paths:
            return jsonify({'error': 'CSVファイルがありません'}), 400
        
        processor = DataProcessor()
        combined_data = processor.load_and_combine_csv_files(file_paths)
        min_date, max_date = processor.get_date_range(combined_data)
        
        # 処理済みデータを一時ファイルに保存
        processed_filename = f"{uuid.uuid4().hex}.pkl"
        processed_data_path = os.path.join(app.config['PROCESSED_DATA_FOLDER'], processed_filename)
        with open(processed_data_path, 'wb') as f:
            pickle.dump(combined_data, f)
            
        session['processed_data_path'] = processed_data_path
        session['min_date'] = min_date
        session['max_date'] = max_date
        session['analysis_performed'] = False # 分析はまだ実行されていない
        
        # 元のアップロードファイルは不要になったら削除することも検討 (ディスク容量節約のため)
        # for p in file_paths:
        #     if os.path.exists(p):
        #         os.remove(p)

        # アップロード成功時に analysis_results_path が残っていればクリア
        if session.get('analysis_results_path'):
            if os.path.exists(session['analysis_results_path']):
                try:
                    os.remove(session['analysis_results_path'])
                    app.logger.info(f"アップロード時に古い分析結果ファイルを削除: {session['analysis_results_path']}")
                except OSError as e:
                    app.logger.error(f"アップロード時の古い分析結果ファイル削除失敗: {session['analysis_results_path']}, エラー: {e}")
            session.pop('analysis_results_path', None)

        return jsonify({
            'success': True,
            'message': f'{len(file_paths)}個のCSVファイルを読み込み、処理しました',
            'total_records': len(combined_data),
            'min_date': min_date,
            'max_date': max_date
        })
    
    except Exception as e:
        app.logger.error(f"ファイルアップロードエラー: {traceback.format_exc()}")
        return jsonify({'error': f'ファイル処理エラーが発生しました: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze_data():
    """リピート分析実行"""
    try:
        processed_data_path = session.get('processed_data_path')
        if not processed_data_path or not os.path.exists(processed_data_path):
            app.logger.error("分析試行: 処理済みデータが見つかりません。")
            return jsonify({'error': '処理済みのデータが見つかりません。ファイルを再アップロードしてください。'}), 400

        with open(processed_data_path, 'rb') as f:
            raw_data = pickle.load(f)

        # リクエストパラメータ取得
        new_customer_start = request.json.get('new_customer_start')
        new_customer_end = request.json.get('new_customer_end')
        repeat_analysis_end = request.json.get('repeat_analysis_end')
        
        TARGET_RATES = {
            'first_repeat': 35.0,
            'second_repeat': 45.0,
            'third_repeat': 60.0
        }
        
        min_repeat_count = int(request.json.get('min_repeat_count', 3))
        min_stylist_customers = int(request.json.get('min_stylist_customers', 10))
        min_coupon_customers = int(request.json.get('min_coupon_customers', 5))
        
        analyzer = RepeatAnalyzer()
        analysis_results_data = analyzer.analyze_repeat_customers(
            raw_data,
            new_customer_start,
            new_customer_end,
            repeat_analysis_end,
            min_repeat_count=min_repeat_count,
            min_stylist_customers=min_stylist_customers,
            min_coupon_customers=min_coupon_customers,
            target_rates=TARGET_RATES
        )
        
        # 分析結果を一時ファイルに保存
        analysis_results_filename = f"{uuid.uuid4().hex}_analysis.pkl"
        analysis_results_path = os.path.join(app.config['PROCESSED_DATA_FOLDER'], analysis_results_filename)
        with open(analysis_results_path, 'wb') as f:
            pickle.dump(analysis_results_data, f)
            
        session['analysis_results_path'] = analysis_results_path
        
        session['analysis_parameters'] = {
            'new_customer_start': new_customer_start,
            'new_customer_end': new_customer_end,
            'repeat_analysis_end': repeat_analysis_end,
            'min_repeat_count': min_repeat_count,
            'min_stylist_customers': min_stylist_customers,
            'min_coupon_customers': min_coupon_customers,
            'target_rates': TARGET_RATES
        }
        session['analysis_performed'] = True
        
        return jsonify({
            'success': True,
            'message': '分析が完了しました',
            'redirect_url': url_for('dashboard')
        })
    
    except Exception as e:
        app.logger.error(f"分析エラー: {traceback.format_exc()}")
        return jsonify({'error': f'分析中にエラーが発生しました: {str(e)}'}), 500

@app.route('/dashboard')
def dashboard():
    """ダッシュボード画面"""
    if not session.get('analysis_performed', False):
        app.logger.warning("ダッシュボードアクセス試行: 分析が実行されていません。")
        return redirect(url_for('index'))

    try:
        analysis_results_path = session.get('analysis_results_path')
        analysis_parameters = session.get('analysis_parameters')

        if not analysis_results_path or not os.path.exists(analysis_results_path) or not analysis_parameters:
            app.logger.error("ダッシュボード表示エラー: セッションに分析結果パスまたはパラメータがありません。")
            return render_template('error.html', message='分析データがセッションに見つかりません。再分析してください。')
        
        with open(analysis_results_path, 'rb') as f:
            analysis_results = pickle.load(f)
            
        visualizer = DashboardVisualizer()
        dashboard_data = visualizer.generate_dashboard_data(analysis_results)
        
        return render_template('dashboard.html', 
                             data=dashboard_data,
                             parameters=analysis_parameters)
    except Exception as e:
        app.logger.error(f"ダッシュボード表示エラー: {traceback.format_exc()}")
        return render_template('error.html', message=f'ダッシュボード表示中にエラーが発生しました: {str(e)}')

@app.route('/report')
def generate_report():
    """レポート生成・ダウンロード"""
    if not session.get('analysis_performed', False):
        app.logger.warning("レポート生成試行: 分析が実行されていません。")
        return render_template('error.html', message='レポートを生成するには、まず分析を実行してください。')

    try:
        analysis_results_path = session.get('analysis_results_path')
        analysis_parameters = session.get('analysis_parameters')

        if not analysis_results_path or not os.path.exists(analysis_results_path) or not analysis_parameters:
            app.logger.error("レポート生成エラー: セッションに分析結果パスまたはパラメータがありません。")
            return render_template('error.html', message='分析データがセッションに見つかりません。再分析してください。')
        
        with open(analysis_results_path, 'rb') as f:
            analysis_results = pickle.load(f)
            
        generator = ReportGenerator()
        app.logger.info(f"レポート生成開始: parameters={json.dumps(analysis_parameters, indent=2, ensure_ascii=False)}")
        report_path = generator.generate_text_report(
            analysis_results,
            analysis_parameters
        )
        app.logger.info(f"レポートファイルパス: {report_path}")
        
        if not os.path.exists(report_path):
            app.logger.error(f"生成されたレポートファイルが見つかりません: {report_path}")
            return render_template('error.html', message=f'生成されたレポートファイルが見つかりません: {report_path}')
            
        return send_file(report_path, 
                        as_attachment=True,
                        # download_nameは送信時に設定されるため、固定名で良い場合もある
                        download_name=f'リピート分析レポート_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    
    except Exception as e:
        app.logger.error(f"レポート生成エラー: {traceback.format_exc()}")
        return render_template('error.html', error_message="レポートの生成中にエラーが発生しました。"), 500

@app.errorhandler(404)
def page_not_found(e):
    app.logger.warning(f"404エラー: {request.url} (Referrer: {request.referrer})")
    return render_template('error.html', message='お探しのページは見つかりませんでした。', error_code=404), 404

@app.errorhandler(500) # 汎用エラーハンドラの前に定義
def internal_server_error_specific(e):
    app.logger.error(f"500エラー (Specific Handler): {traceback.format_exc()}")
    return render_template('error.html', message='サーバー内部で予期せぬエラーが発生しました。', error_code=500), 500

@app.errorhandler(Exception) # 汎用エラーハンドラ
def handle_exception(e):
    # HTTP例外はwerkzeugが処理するので、それ以外のPython例外を主に捕捉
    app.logger.error(f"予期せぬエラー: {traceback.format_exc()}")
    # エラーの内容によっては詳細をユーザーに見せるべきでない場合もある
    return render_template('error.html', message='処理中に予期せぬエラーが発生しました。'), 500

if __name__ == '__main__':
    # debug=True は開発時のみ。本番環境ではFalseに。
    app.run(debug=True, host='0.0.0.0', port=5001) 