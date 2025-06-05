#!/usr/bin/env python3
"""
美容室顧客データリピート分析システム - メインアプリケーション
"""

import os
from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename
import pandas as pd
import json
from datetime import datetime, timedelta
import traceback

# カスタムモジュールのインポート
from modules.data_processor import DataProcessor
from modules.repeat_analyzer import RepeatAnalyzer
from modules.visualization import DashboardVisualizer
from modules.report_generator import ReportGenerator

app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB max file size

# アップロードフォルダを作成
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# グローバル変数（セッション管理の代替）
current_analysis_data = {}

@app.route('/')
def index():
    """メインページ - ファイルアップロードと設定画面"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """CSVファイルのアップロード処理"""
    try:
        uploaded_files = request.files.getlist('csv_files')
        if not uploaded_files or not uploaded_files[0].filename:
            return jsonify({'error': 'ファイルが選択されていません'}), 400
        
        file_paths = []
        for file in uploaded_files:
            if file and file.filename.endswith('.csv'):
                filename = secure_filename(file.filename)
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(file_path)
                file_paths.append(file_path)
        
        if not file_paths:
            return jsonify({'error': 'CSVファイルがありません'}), 400
        
        # データ処理
        processor = DataProcessor()
        combined_data = processor.load_and_combine_csv_files(file_paths)
        
        # グローバル変数に保存
        global current_analysis_data
        current_analysis_data['raw_data'] = combined_data
        current_analysis_data['file_paths'] = file_paths
        
        return jsonify({
            'success': True,
            'message': f'{len(file_paths)}個のCSVファイルを読み込みました',
            'total_records': len(combined_data)
        })
    
    except Exception as e:
        return jsonify({'error': f'ファイル処理エラー: {str(e)}'}), 500

@app.route('/analyze', methods=['POST'])
def analyze_data():
    """リピート分析実行"""
    try:
        # リクエストパラメータ取得
        new_customer_start = request.json.get('new_customer_start')
        new_customer_end = request.json.get('new_customer_end')
        repeat_analysis_end = request.json.get('repeat_analysis_end')
        
        # 目標値パラメータ
        target_first_repeat = float(request.json.get('target_first_repeat', 35))
        target_second_repeat = float(request.json.get('target_second_repeat', 45))
        target_third_repeat = float(request.json.get('target_third_repeat', 60))
        
        # 分析パラメータ
        min_repeat_count = int(request.json.get('min_repeat_count', 3))
        min_stylist_customers = int(request.json.get('min_stylist_customers', 10))
        min_coupon_customers = int(request.json.get('min_coupon_customers', 5))
        
        if 'raw_data' not in current_analysis_data:
            return jsonify({'error': 'データが読み込まれていません'}), 400
        
        # 分析実行
        analyzer = RepeatAnalyzer()
        analysis_results = analyzer.analyze_repeat_customers(
            current_analysis_data['raw_data'],
            new_customer_start,
            new_customer_end,
            repeat_analysis_end,
            min_repeat_count=min_repeat_count,
            min_stylist_customers=min_stylist_customers,
            min_coupon_customers=min_coupon_customers,
            target_rates={
                'first_repeat': target_first_repeat,
                'second_repeat': target_second_repeat,
                'third_repeat': target_third_repeat
            }
        )
        
        current_analysis_data['results'] = analysis_results
        current_analysis_data['parameters'] = {
            'new_customer_start': new_customer_start,
            'new_customer_end': new_customer_end,
            'repeat_analysis_end': repeat_analysis_end,
            'min_repeat_count': min_repeat_count,
            'min_stylist_customers': min_stylist_customers,
            'min_coupon_customers': min_coupon_customers,
            'target_rates': {
                'first_repeat': target_first_repeat,
                'second_repeat': target_second_repeat,
                'third_repeat': target_third_repeat
            }
        }
        
        return jsonify({
            'success': True,
            'message': '分析が完了しました',
            'redirect_url': '/dashboard'
        })
    
    except Exception as e:
        return jsonify({'error': f'分析エラー: {str(e)}'}), 500

@app.route('/dashboard')
def dashboard():
    """ダッシュボード画面"""
    if 'results' not in current_analysis_data:
        return render_template('error.html', message='分析データがありません')
    
    # ダッシュボード用データ生成
    visualizer = DashboardVisualizer()
    dashboard_data = visualizer.generate_dashboard_data(current_analysis_data['results'])
    
    return render_template('dashboard.html', 
                         data=dashboard_data,
                         parameters=current_analysis_data['parameters'])

@app.route('/api/chart/<chart_type>')
def get_chart_data(chart_type):
    """チャートデータAPI"""
    if 'results' not in current_analysis_data:
        return jsonify({'error': 'データがありません'}), 400
    
    visualizer = DashboardVisualizer()
    chart_data = visualizer.get_chart_data(chart_type, current_analysis_data['results'])
    
    return jsonify(chart_data)

@app.route('/report')
def generate_report():
    """レポート生成・ダウンロード"""
    if 'results' not in current_analysis_data:
        app.logger.error("レポート生成試行: 分析データが見つかりません")
        return jsonify({'error': 'データがありません'}), 400
    
    try:
        generator = ReportGenerator()
        app.logger.info(f"レポート生成開始: parameters={current_analysis_data.get('parameters')}")
        report_path = generator.generate_text_report(
            current_analysis_data['results'],
            current_analysis_data.get('parameters', {})
        )
        app.logger.info(f"レポートファイルパス: {report_path}")
        
        if not os.path.exists(report_path):
            app.logger.error(f"レポートファイルが見つかりません: {report_path}")
            return jsonify({'error': f'生成されたレポートファイルが見つかりません: {report_path}'}), 500
            
        return send_file(report_path, 
                        as_attachment=True,
                        download_name=f'リピート分析レポート_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    
    except Exception as e:
        detailed_error = traceback.format_exc()
        app.logger.error(f"レポート生成エラー: {str(e)}\n{detailed_error}")
        return jsonify({'error': f'レポート生成エラー: {str(e)}'}), 500

@app.errorhandler(404)
def page_not_found(e):
    return render_template('error.html', message='ページが見つかりません'), 404

@app.errorhandler(500)
def internal_server_error(e):
    return render_template('error.html', message='サーバー内部エラーが発生しました'), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000) 