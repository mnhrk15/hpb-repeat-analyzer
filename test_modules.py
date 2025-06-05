#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
テストスクリプト: データ処理モジュールのテスト
"""

import sys
import traceback
from datetime import datetime
import pandas as pd # pandas を明示的にインポート
import io # io をインポート
import os # os をインポート（一時ファイル作成のため）
import tempfile # tempfile をインポート

from modules.data_processor import DataProcessor
from modules.repeat_analyzer import RepeatAnalyzer
from modules.visualization import DashboardVisualizer
from modules.report_generator import ReportGenerator

# テストで使用する一時ファイルを作成・管理するヘルパー
def create_temp_csv_file(content, encoding='utf-8', prefix='test_csv_', dir=None):
    fd, path = tempfile.mkstemp(prefix=prefix, suffix='.csv', dir=dir, text=False)
    with os.fdopen(fd, 'wb') as tmp_file:
        tmp_file.write(content.encode(encoding))
    return path

def test_data_processor():
    """データプロセッサのテスト（拡張版）"""
    print("=" * 50)
    print("データプロセッサテスト開始")
    print("=" * 50)
    
    all_tests_passed = True
    final_df_for_next_step = None

    # --- シナリオ1: 基本的なUTF-8 CSV の処理 --- 
    print("\n--- シナリオ1: 基本的なUTF-8 CSV --- ")
    csv_content_s1 = (
        "来店日,ステータス,顧客ID,お名前,電話番号,売上,このサロンに行くのは初めてですか？,スタイリスト名,予約時メニュー,予約時HotPepperBeautyクーポン,予約時合計金額,性別\n"
        "2024-01-01,済み,C001,山田 花子,090-1234-5678,5000,True,指名なし,カット,クーポンA,5000,女性\n"
        "2024-01-15,済み,C001,山田 ハナコ,090-1234-5678,6000,False,佐藤,カット+カラー,クーポンB,10000,女性\n"
        "2024/02/10,済み,C002,佐藤 次郎,080-9876-5432,7500,True,鈴木,パーマ,なし,7500,男性\n"
        "20240320,済み,C003,田中 三郎,,3000,True,指名なし,トリートメント,クーポンC,3000,未設定\n"
        "invalid-date,済み,C004,鈴木 四郎,070-1111-2222,0,True,佐藤,カット,なし,0,男性\n"
        "2024-04-05,キャンセル,C005,伊藤 五郎,070-3333-4444,5000,True,鈴木,ヘッドスパ,クーポンD,5000,女性\n"
    )
    temp_file_s1 = None
    try:
        processor_s1 = DataProcessor()
        print("[S1] ✅ DataProcessor初期化成功")
        temp_file_s1 = create_temp_csv_file(csv_content_s1, encoding='utf-8')
        df_s1 = processor_s1.load_and_combine_csv_files([temp_file_s1])
        
        print(f"[S1] CSV読み込み・処理後の件数: {len(df_s1)}件")
        
        # 検証1: ステータス '済み' のみ、かつ有効な日付のレコードが残る
        # 山田(2件), 佐藤(1件), 田中(1件) の計4件のはず (invalid-dateとキャンセルは除外)
        expected_rows_s1 = 4
        if len(df_s1) == expected_rows_s1:
            print(f"[S1] ✅ 行数チェックOK ({len(df_s1)}件)")
        else:
            print(f"[S1] ❌ 行数チェックNG: 期待値={expected_rows_s1}, 実際={len(df_s1)}")
            all_tests_passed = False

        # 検証2: '来店日_parsed' カラムが datetime 型であること（現在は DataProcessor が '来店日'を上書き）
        if not df_s1.empty and pd.api.types.is_datetime64_any_dtype(df_s1['来店日']):
            print("[S1] ✅ '来店日'カラムの型チェックOK")
        elif not df_s1.empty:
            print(f"[S1] ❌ '来店日'カラムの型チェックNG: 型={df_s1['来店日'].dtype}")
            all_tests_passed = False

        # 検証3: 顧客同定の確認 (山田花子は同一人物として扱われるか？)
        # DataProcessorの現在の顧客ID生成ロジックに依存する
        # 電話番号が同じであれば同じ顧客ID (PHONE_09012345678) になるはず
        if not df_s1.empty and '顧客ID' in df_s1.columns: # DataProcessor は '顧客ID' カラムを生成
            yamada_records_s1 = df_s1[df_s1['お名前'].str.contains('山田', na=False)]
            if not yamada_records_s1.empty and yamada_records_s1['顧客ID'].nunique() == 1:
                print(f"[S1] ✅ 山田様の顧客ID統一チェックOK (ID: {yamada_records_s1['顧客ID'].iloc[0]})")
            elif not yamada_records_s1.empty:
                print(f"[S1] ❌ 山田様の顧客ID統一チェックNG: ユニークID数={yamada_records_s1['顧客ID'].nunique()}")
                all_tests_passed = False
        else:
            print("[S1] ⚠️ '顧客ID' カラムが見つからないため、顧客同定チェックをスキップ")
        
        # print("[S1] 処理後データサンプル:")
        # print(df_s1.head())

        final_df_for_next_step = df_s1 # 最初の成功したDFを次のステップ用に保持

    except Exception as e:
        print(f"[S1] ❌ シナリオ1テスト失敗: {e}")
        traceback.print_exc()
        all_tests_passed = False
    finally:
        if temp_file_s1 and os.path.exists(temp_file_s1):
            os.remove(temp_file_s1)

    # --- シナリオ2: Shift_JIS CSV の処理 --- 
    print("\n--- シナリオ2: Shift_JIS CSV --- ")
    csv_content_s2 = (
        "来店日,ステータス,顧客ID,お名前,電話番号,売上,このサロンに行くのは初めてですか？\n"
        "2023-05-01,済み,SJ001,鈴木 一郎,070-0000-0001,8000,はい、初めてです\n"
        "2023-05-20,済み,SJ001,ｽｽﾞｷ ｲﾁﾛｳ,07000000001,9000,いいえ\n"
    )
    temp_file_s2 = None
    try:
        processor_s2 = DataProcessor() # default_encoding は指定せず自動検出に任せる
        print("[S2] ✅ DataProcessor初期化成功")
        temp_file_s2 = create_temp_csv_file(csv_content_s2, encoding='cp932') # Shift_JIS系として保存
        df_s2 = processor_s2.load_and_combine_csv_files([temp_file_s2])
        
        expected_rows_s2 = 2
        if len(df_s2) == expected_rows_s2:
            print(f"[S2] ✅ Shift_JIS CSV読み込み・行数チェックOK ({len(df_s2)}件)")
        else:
            print(f"[S2] ❌ Shift_JIS CSV読み込み・行数チェックNG: 期待値={expected_rows_s2}, 実際={len(df_s2)}")
            all_tests_passed = False
        
        if not df_s2.empty and df_s2[df_s2['お名前'].str.contains('鈴木', na=False)]['顧客ID'].nunique() == 1:
            print(f"[S2] ✅ 鈴木様の顧客ID統一チェックOK")
        elif not df_s2.empty:
             print(f"[S2] ❌ 鈴木様の顧客ID統一チェックNG")
             all_tests_passed = False

        # 他のモジュールのために、最後に読み込んだ有効なdfをfinal_df_for_next_stepに設定するか検討
        # if final_df_for_next_step is None and not df_s2.empty: 
        #     final_df_for_next_step = df_s2

    except Exception as e:
        print(f"[S2] ❌ シナリオ2テスト失敗: {e}")
        traceback.print_exc()
        all_tests_passed = False
    finally:
        if temp_file_s2 and os.path.exists(temp_file_s2):
            os.remove(temp_file_s2)
    
    # --- シナリオ3: 実際のサンプルファイル (もしあれば) --- 
    # 既存のテストファイルも実行してみる (パスが正しければ)
    sample_file_path = 'test_data/sample_data.csv' # このパスは環境に依存
    print(f"\n--- シナリオ3: 既存サンプルファイル ({sample_file_path}) --- ")
    if os.path.exists(sample_file_path):
        try:
            # default_encoding='cp932' は元のテストの指定を尊重
            processor_s3 = DataProcessor(default_encoding='cp932') 
            df_s3 = processor_s3.load_and_combine_csv_files([sample_file_path])
            print(f"[S3] ✅ 既存サンプルデータ読み込み・処理成功: {len(df_s3)}件のレコード")
            print(f"[S3] カラム数: {len(df_s3.columns)}")
            if not df_s3.empty and '顧客ID' in df_s3.columns:
                 print(f"[S3] 顧客数: {df_s3['顧客ID'].nunique()}人")
            
            if final_df_for_next_step is None and not df_s3.empty:
                final_df_for_next_step = df_s3

        except Exception as e:
            print(f"[S3] ❌ 既存サンプルファイル処理失敗: {e}")
            # traceback.print_exc() # 既存ファイルなのでエラー詳細は抑制してもよい
            all_tests_passed = False # サンプルファイル処理失敗もテスト失敗とカウントするかどうか
    else:
        print(f"[S3] ⚠️ 既存サンプルファイル {sample_file_path} が見つかりません。このテストはスキップします。")

    # --- テスト結果の集約 --- 
    print("\n" + "-" * 30)
    if all_tests_passed:
        print("✅✅✅ データプロセッサ 全シナリオ成功 ✅✅✅")
    else:
        print("❌❌❌ データプロセッサ いずれかのシナリオ失敗 ❌❌❌")
    print("-" * 30)

    # 次のテストステップには、いずれかのシナリオで成功したDataFrameを渡す
    # もし全てのシナリオでDataFrame生成に失敗した場合はNoneが渡される
    if final_df_for_next_step is not None:
        print(f"次のテストステップへ {len(final_df_for_next_step)}件のDataFrameを渡します。")
    else:
        print("⚠️ 有効なDataFrameが生成されなかったため、次のテストステップへはNoneを渡します。")
        
    return final_df_for_next_step # 変更：成功したDFまたはNoneを返す

def test_repeat_analyzer(df_processed):
    """リピート分析モジュールのテスト（拡張版）"""
    print("\n" + "=" * 50)
    print("リピート分析モジュールテスト開始")
    print("=" * 50)
    
    if df_processed is None or df_processed.empty:
        print("❌ リピート分析テスト: 入力データがありません。テストをスキップします。")
        return None # 失敗を示すためにNoneを返す

    analyzer = RepeatAnalyzer()
    print("✅ RepeatAnalyzer初期化成功")
    all_tests_passed = True
    final_repeat_data = None

    # --- シナリオ1: 通常ケース (test_data_processorのS1データに基づく) ---
    print("\n--- リピート分析 シナリオ1: 通常ケース ---")
    # test_data_processor のシナリオ1データ想定:
    # C001 (山田) 2024-01-01 (新規), 2024-01-15 (リピート)
    # C002 (佐藤) 2024-02-10 (新規)
    # C003 (田中) 2024-03-20 (新規)
    # DataProcessorによって '来店日' -> '来店日_parsed' になっている想定
    # また、DataProcessor.get_new_customers が '来店日_parsed' を '来店日' として返すのを
    # RepeatAnalyzer._analyze_repeat_patterns が再度 '初回来店日' にリネームする。
    # ここでは df_processed が DataProcessor の出力そのもの (つまり '来店日_parsed' を持つ) と仮定。

    # DataProcessor の出力に合わせてカラム名を調整 (テスト用に)
    # RepeatAnalyzer は get_new_customers を内部で呼ぶので、df_processed は get_new_customers の入力
    # get_new_customers は '来店日_parsed' を見る。
    # そして、get_new_customers は '来店日_parsed' (これが初来店日) カラムを持つdfを返す。

    # df_processed のカラム確認 (デバッグ用)
    # print("[RepeatAnalyzer Test S1] 入力df_processedのカラム:", df_processed.columns)
    # print("[RepeatAnalyzer Test S1] 入特定顧客のデータサンプル:")
    # if not df_processed[df_processed['顧客ID'].str.contains('C001', na=False)].empty:
    #     print(df_processed[df_processed['顧客ID'].str.contains('C001', na=False)])
    # else:
    #     print("C001 が見つかりません")

    # RepeatAnalyzer の入力 df は '来店日' カラムがdatetime型になっている想定
    # DataProcessor.get_new_customers メソッドも '来店日' カラムを期待する。
    # 以前のテストコードではここで '来店日' を '来店日_parsed' にリネームしていたが、
    # DataProcessor側の修正により '来店日' カラムがdatetime型で提供されるため、このリネームは不要。
    current_df_for_analyzer = df_processed.copy()
    # if '来店日' in current_df_for_analyzer.columns and '来店日_parsed' not in current_df_for_analyzer.columns:
    #     current_df_for_analyzer.rename(columns={'来店日': '来店日_parsed'}, inplace=True)
    #     print("[RepeatAnalyzer Test S1] '来店日' を '来店日_parsed' にリネームしました。(この処理は将来削除されるべき)")
    
    if '来店日' not in current_df_for_analyzer.columns:
        print("❌ [RepeatAnalyzer Test S1] 入力DFに '来店日' カラムがありません。")
        all_tests_passed = False
        # このシナリオはこれ以上進めない
        return None 
    elif not pd.api.types.is_datetime64_any_dtype(current_df_for_analyzer['来店日']):
        print(f"❌ [RepeatAnalyzer Test S1] 入力DFの '来店日' カラムがdatetime型ではありません。型: {current_df_for_analyzer['来店日'].dtype}")
        all_tests_passed = False
        return None

    # DataProcessor.get_new_customers が返す 'new_customers' DataFrame は、
    # '顧客ID', '来店日_parsed' (これが初来店日), 'スタイリスト名' などを含む。
    # これをシミュレートして RepeatAnalyzer._analyze_repeat_patterns に渡すのではなく、
    # analyze_repeat_customers に全データ(current_df_for_analyzer)を渡す。

    try:
        new_customer_start_s1 = '2024-01-01'
        new_customer_end_s1 = '2024-01-31' # 山田(C001)がこの期間の新規顧客
        repeat_analysis_end_s1 = '2024-03-31' # 山田のリピート(01-15)と田中(03-20)の新規は含む
        
        print(f"[RepeatAnalyzer Test S1] 分析パラメータ: 新規顧客期間={new_customer_start_s1}～{new_customer_end_s1}, リピート集計終了日={repeat_analysis_end_s1}")

        repeat_data_s1 = analyzer.analyze_repeat_customers(
            current_df_for_analyzer, 
            new_customer_start=new_customer_start_s1, 
            new_customer_end=new_customer_end_s1,
            repeat_analysis_end=repeat_analysis_end_s1,
            min_repeat_count=1 # テストしやすくするため
        )
        print("[RepeatAnalyzer Test S1] ✅ analyze_repeat_customers 実行成功")
        final_repeat_data = repeat_data_s1 # 後続テストのために保持

        # 結果の検証
        if not isinstance(repeat_data_s1, dict):
            print("❌ [RepeatAnalyzer Test S1] 結果が辞書ではありません。")
            all_tests_passed = False
        else:
            # Basic Stats
            bs = repeat_data_s1.get('basic_stats', {})
            expected_total_new_s1 = 1 # 山田(C001)のみ
            if bs.get('total_new_customers') == expected_total_new_s1:
                print(f"[RepeatAnalyzer Test S1] ✅ basic_stats.total_new_customers: {bs.get('total_new_customers')} (期待値: {expected_total_new_s1})")
            else:
                print(f"❌ [RepeatAnalyzer Test S1] basic_stats.total_new_customers: {bs.get('total_new_customers')} (期待値: {expected_total_new_s1})")
                all_tests_passed = False

            expected_first_repeaters_s1 = 1 # 山田(C001)が1回リピート
            if bs.get('first_repeaters') == expected_first_repeaters_s1:
                print(f"[RepeatAnalyzer Test S1] ✅ basic_stats.first_repeaters: {bs.get('first_repeaters')} (期待値: {expected_first_repeaters_s1})")
            else:
                print(f"❌ [RepeatAnalyzer Test S1] basic_stats.first_repeaters: {bs.get('first_repeaters')} (期待値: {expected_first_repeaters_s1})")
                all_tests_passed = False
            
            expected_first_rate_s1 = 100.0
            if bs.get('first_repeat_rate') == expected_first_rate_s1:
                 print(f"[RepeatAnalyzer Test S1] ✅ basic_stats.first_repeat_rate: {bs.get('first_repeat_rate')} (期待値: {expected_first_rate_s1})")
            else:
                print(f"❌ [RepeatAnalyzer Test S1] basic_stats.first_repeat_rate: {bs.get('first_repeat_rate')} (期待値: {expected_first_rate_s1})")
                all_tests_passed = False

            # Funnel Analysis
            fa = repeat_data_s1.get('funnel_analysis', {}).get('stages', {})
            if fa.get('新規来店') == expected_total_new_s1 and fa.get('2回目来店') == expected_first_repeaters_s1:
                print(f"[RepeatAnalyzer Test S1] ✅ funnel_analysis.stages: 新規={fa.get('新規来店')}, 2回目={fa.get('2回目来店')}")
            else:
                print(f"❌ [RepeatAnalyzer Test S1] funnel_analysis.stages: 新規={fa.get('新規来店')}, 2回目={fa.get('2回目来店')} (期待値 新規={expected_total_new_s1}, 2回目={expected_first_repeaters_s1})")
                all_tests_passed = False

            # パラメータ確認
            params = repeat_data_s1.get('parameters', {})
            if params.get('new_customer_start') == new_customer_start_s1:
                print("[RepeatAnalyzer Test S1] ✅ parameters.new_customer_start OK")
            else:
                print("❌ [RepeatAnalyzer Test S1] parameters.new_customer_start NG")
                all_tests_passed = False
                
    except Exception as e:
        print(f"❌ [RepeatAnalyzer Test S1] シナリオ1テスト中にエラー: {e}")
        traceback.print_exc()
        all_tests_passed = False

    # --- シナリオ2: 新規顧客ゼロのケース ---
    print("\n--- リピート分析 シナリオ2: 新規顧客ゼロ --- ")
    try:
        new_customer_start_s2 = '2023-01-01' # テストデータに存在しない期間
        new_customer_end_s2 = '2023-01-31'
        repeat_analysis_end_s2 = '2023-03-31'
        print(f"[RepeatAnalyzer Test S2] 分析パラメータ: 新規顧客期間={new_customer_start_s2}～{new_customer_end_s2}, リピート集計終了日={repeat_analysis_end_s2}")

        repeat_data_s2 = analyzer.analyze_repeat_customers(
            current_df_for_analyzer, 
            new_customer_start=new_customer_start_s2, 
            new_customer_end=new_customer_end_s2,
            repeat_analysis_end=repeat_analysis_end_s2
        )
        print("[RepeatAnalyzer Test S2] ✅ analyze_repeat_customers 実行成功 (新規顧客ゼロ想定)")

        params_s2 = repeat_data_s2.get('parameters', {})
        if 'error' in params_s2 and "新規顧客が見つかりません" in params_s2['error']:
            print(f"[RepeatAnalyzer Test S2] ✅ 新規顧客ゼロの場合のエラーメッセージ確認OK: {params_s2['error']}")
        else:
            print(f"❌ [RepeatAnalyzer Test S2] 新規顧客ゼロの場合のエラーメッセージ確認NG: {params_s2.get('error')}")
            all_tests_passed = False
        
        # 基本統計が空であること（またはそれに準ずる状態）
        bs_s2 = repeat_data_s2.get('basic_stats', {})
        if not bs_s2: # 空の辞書であることを期待
            print(f"[RepeatAnalyzer Test S2] ✅ basic_statsが空であること確認OK")
        else:
            print(f"❌ [RepeatAnalyzer Test S2] basic_statsが空ではありません: {bs_s2}")
            all_tests_passed = False

    except Exception as e:
        print(f"❌ [RepeatAnalyzer Test S2] シナリオ2テスト中にエラー: {e}")
        traceback.print_exc()
        all_tests_passed = False

    # --- シナリオ3: リピートゼロのケース ---
    print("\n--- リピート分析 シナリオ3: リピートゼロ --- ")
    # C002 (佐藤) 2024-02-10 が新規顧客。リピート集計終了日をその直後に設定。
    try:
        new_customer_start_s3 = '2024-02-01'
        new_customer_end_s3 = '2024-02-28' # 佐藤(C002)がこの期間の新規顧客
        repeat_analysis_end_s3 = '2024-02-15' # 佐藤の初回来店(02-10)以降リピートなし
        print(f"[RepeatAnalyzer Test S3] 分析パラメータ: 新規顧客期間={new_customer_start_s3}～{new_customer_end_s3}, リピート集計終了日={repeat_analysis_end_s3}")

        repeat_data_s3 = analyzer.analyze_repeat_customers(
            current_df_for_analyzer, 
            new_customer_start=new_customer_start_s3, 
            new_customer_end=new_customer_end_s3,
            repeat_analysis_end=repeat_analysis_end_s3
        )
        print("[RepeatAnalyzer Test S3] ✅ analyze_repeat_customers 実行成功 (リピートゼロ想定)")

        bs_s3 = repeat_data_s3.get('basic_stats', {})
        expected_total_new_s3 = 1 # 佐藤(C002)
        if bs_s3.get('total_new_customers') == expected_total_new_s3:
            print(f"[RepeatAnalyzer Test S3] ✅ total_new_customers: {bs_s3.get('total_new_customers')} (期待値: {expected_total_new_s3})")
        else:
            print(f"❌ [RepeatAnalyzer Test S3] total_new_customers: {bs_s3.get('total_new_customers')} (期待値: {expected_total_new_s3})")
            all_tests_passed = False

        expected_first_repeaters_s3 = 0 # リピートなし
        if bs_s3.get('first_repeaters') == expected_first_repeaters_s3:
            print(f"[RepeatAnalyzer Test S3] ✅ first_repeaters: {bs_s3.get('first_repeaters')} (期待値: {expected_first_repeaters_s3})")
        else:
            print(f"❌ [RepeatAnalyzer Test S3] first_repeaters: {bs_s3.get('first_repeaters')} (期待値: {expected_first_repeaters_s3})")
            all_tests_passed = False

        expected_first_rate_s3 = 0.0
        if bs_s3.get('first_repeat_rate') == expected_first_rate_s3:
             print(f"[RepeatAnalyzer Test S3] ✅ first_repeat_rate: {bs_s3.get('first_repeat_rate')} (期待値: {expected_first_rate_s3})")
        else:
            print(f"❌ [RepeatAnalyzer Test S3] first_repeat_rate: {bs_s3.get('first_repeat_rate')} (期待値: {expected_first_rate_s3})")
            all_tests_passed = False

    except Exception as e:
        print(f"❌ [RepeatAnalyzer Test S3] シナリオ3テスト中にエラー: {e}")
        traceback.print_exc()
        all_tests_passed = False

    # --- テスト結果の集約 --- 
    print("\n" + "-" * 30)
    if all_tests_passed:
        print("✅✅✅ リピート分析 全シナリオ成功 ✅✅✅")
    else:
        print("❌❌❌ リピート分析 いずれかのシナリオ失敗 ❌❌❌")
    print("-" * 30)
    
    # 最初の成功した分析結果 (final_repeat_data) を後続のテストに渡す
    # もし全てのシナリオで分析失敗、またはS1が失敗した場合はNoneになる可能性あり
    if final_repeat_data is not None:
        print(f"次のテストステップへリピート分析データ ({len(final_repeat_data)} keys) を渡します。")
    else:
        print("⚠️ 有効なリピート分析データが生成されなかったため、次のテストステップへはNoneを渡します。")

    return final_repeat_data if all_tests_passed else None # 全テスト成功時のみデータを返す

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
    
    processed_df = test_data_processor() # 名前を変更 df -> processed_df
    
    # test_data_processor が成功した (Noneでない) 場合のみ後続のテストを実行
    if processed_df is not None and not processed_df.empty:
        repeat_data = test_repeat_analyzer(processed_df)
        viz_success = test_visualization(repeat_data)
        report_success = test_report_generator(repeat_data)
    else:
        print("データ処理に失敗したため、後続の分析・可視化・レポート生成テストをスキップします。")
        repeat_data = None
        viz_success = False
        report_success = False
    
    print("\n" + "=" * 50)
    print("テスト結果サマリー")
    print("=" * 50)
    
    results = [
        ("データプロセッサ", processed_df is not None and not processed_df.empty), # 成功条件を明確化
        ("リピート分析", repeat_data is not None),
        ("可視化", viz_success),
        ("レポート生成", report_success)
    ]
    
    passed_count = sum(1 for _, success in results if success)
    total_tests = len(results)
    
    for name, success in results:
        status = "✅ 成功" if success else "❌ 失敗"
        print(f"{name}: {status}")
    
    print(f"\n総合結果: {passed_count}/{total_tests} テスト通過")
    print(f"テスト終了時刻: {datetime.now()}")
    
    # 全ての主要ステップが成功した場合のみ終了コード0を返すように変更も検討
    # ここでは、データプロセッサが失敗したら全体失敗とはせず、個々の結果を見る
    # 終了コードは、全てのテストが「期待通りに」完了したかで判断すべき
    # 今回は、データプロセッサが失敗しても、その事実がレポートされればOKとする
    # スクリプト全体の成功は、クラッシュしなかったことと定義できる
    # より厳密には、全てのresultsがTrueだった場合のみsuccessとする
    all_major_steps_passed = all(success for _, success in results)
    
    return all_major_steps_passed

if __name__ == "__main__":
    overall_success = main()
    sys.exit(0 if overall_success else 1) 