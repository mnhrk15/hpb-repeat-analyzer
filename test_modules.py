#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
テストスクリプト: データ処理モジュールのテスト
"""

import sys
import traceback
from datetime import datetime
from modules.data_processor import DataProcessor
from modules.repeat_analyzer import RepeatAnalyzer
from modules.visualization import DashboardVisualizer
from modules.report_generator import ReportGenerator

def test_data_processor():
    """データプロセッサのテスト"""
    print("=" * 50)
    print("データプロセッサテスト開始")
    print("=" * 50)
    
    try:
        processor = DataProcessor(default_encoding='cp932')
        print("✅ DataProcessor初期化成功")
        
        # テストデータの読み込み（統合処理を含む）
        df = processor.load_and_combine_csv_files(['test_data/sample_data.csv'])
        print(f"✅ テストデータ読み込み・処理成功: {len(df)}件のレコード")
        print(f"カラム数: {len(df.columns)}")
        print(f"顧客数: {len(df['顧客ID'].unique())}人")
        
        return df
        
    except Exception as e:
        print(f"❌ データプロセッサテスト失敗: {e}")
        traceback.print_exc()
        return None

def test_repeat_analyzer(df):
    """リピート分析モジュールのテスト"""
    print("\n" + "=" * 50)
    print("リピート分析モジュールテスト開始")
    print("=" * 50)
    
    if df is None:
        print("❌ テストデータがありません")
        return None
    
    try:
        analyzer = RepeatAnalyzer()
        print("✅ RepeatAnalyzer初期化成功")
        
        # リピート分析（新規顧客特定含む）
        repeat_data = analyzer.analyze_repeat_customers(
            df, 
            new_customer_start='2024-01-01', 
            new_customer_end='2024-01-31',
            repeat_analysis_end='2024-12-31'
        )
        print(f"✅ リピート分析成功")
        print(f"総新規顧客数: {repeat_data['basic_stats']['total_new_customers']}")
        print(f"初回リピーター数: {repeat_data['basic_stats']['first_repeaters']}")
        print(f"初回リピート率: {repeat_data['basic_stats']['first_repeat_rate']:.1f}%")
        
        return repeat_data
        
    except Exception as e:
        print(f"❌ リピート分析テスト失敗: {e}")
        traceback.print_exc()
        return None

def test_visualization(repeat_data):
    """可視化モジュールのテスト"""
    print("\n" + "=" * 50)
    print("可視化モジュールテスト開始")
    print("=" * 50)
    
    if repeat_data is None:
        print("❌ テストデータがありません")
        return False
    
    try:
        visualizer = DashboardVisualizer()
        print("✅ DashboardVisualizer初期化成功")
        
        # ダッシュボードデータ生成テスト
        dashboard_data = visualizer.generate_dashboard_data(repeat_data)
        print("✅ ダッシュボードデータ生成成功")
        
        # ファネルチャートデータの存在確認
        if 'funnel_charts' in dashboard_data and dashboard_data['funnel_charts']:
            print("✅ ファネルチャートデータ確認")
        else:
            print("❌ ファネルチャートデータがありません")
            return False
            
        # スタイリストチャートデータの存在確認 (同様に確認)
        if 'stylist_charts' in dashboard_data and dashboard_data['stylist_charts']:
             print("✅ スタイリストチャートデータ確認")
        else:
            print("❌ スタイリストチャートデータがありません")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ 可視化テスト失敗: {e}")
        traceback.print_exc()
        return False

def test_report_generator(repeat_data):
    """レポート生成モジュールのテスト"""
    print("\n" + "=" * 50)
    print("レポート生成モジュールテスト開始")
    print("=" * 50)
    
    if repeat_data is None:
        print("❌ テストデータがありません")
        return False
    
    try:
        generator = ReportGenerator()
        print("✅ ReportGenerator初期化成功")
        
        # レポート生成テスト (パラメータも渡す)
        report = generator.generate_text_report(repeat_data, repeat_data.get('parameters', {}))
        print("✅ サマリーレポート生成成功")
        print(f"レポート文字数: {len(report)}")
        
        return True
        
    except Exception as e:
        print(f"❌ レポート生成テスト失敗: {e}")
        traceback.print_exc()
        return False

def main():
    """メインテスト関数"""
    print("美容室リピート分析システム - 包括的テスト")
    print(f"テスト開始時刻: {datetime.now()}")
    
    # 各モジュールのテスト実行
    df = test_data_processor()
    repeat_data = test_repeat_analyzer(df)
    viz_success = test_visualization(repeat_data)
    report_success = test_report_generator(repeat_data)
    
    # テスト結果サマリー
    print("\n" + "=" * 50)
    print("テスト結果サマリー")
    print("=" * 50)
    
    results = [
        ("データプロセッサ", df is not None),
        ("リピート分析", repeat_data is not None),
        ("可視化", viz_success),
        ("レポート生成", report_success)
    ]
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for name, success in results:
        status = "✅ 成功" if success else "❌ 失敗"
        print(f"{name}: {status}")
    
    print(f"\n総合結果: {passed}/{total} テスト通過")
    print(f"テスト終了時刻: {datetime.now()}")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 