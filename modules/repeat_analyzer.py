"""
リピート分析モジュール - 新規顧客のリピート状況分析
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Tuple, Optional
from .data_processor import DataProcessor

logger = logging.getLogger(__name__)

class RepeatAnalyzer:
    """リピート分析クラス"""
    
    def __init__(self):
        self.data_processor = DataProcessor()
    
    def analyze_repeat_customers(self, 
                               df: pd.DataFrame,
                               new_customer_start: str,
                               new_customer_end: str, 
                               repeat_analysis_end: str,
                               min_repeat_count: int = 3,
                               min_stylist_customers: int = 10,
                               min_coupon_customers: int = 5,
                               target_rates: Dict[str, float] = None) -> Dict:
        """
        リピート顧客分析のメイン関数
        
        Args:
            df: 全来店データ
            new_customer_start: 新規顧客抽出開始日 (YYYY-MM-DD)
            new_customer_end: 新規顧客抽出終了日 (YYYY-MM-DD)
            repeat_analysis_end: リピート集計終了日 (YYYY-MM-DD)
            min_repeat_count: X回以上リピートの基準回数
            min_stylist_customers: スタイリスト分析の最低顧客数
            min_coupon_customers: クーポン分析の最低顧客数
            target_rates: 目標値辞書
            
        Returns:
            分析結果辞書
        """
        logger.info("リピート分析開始")

        # 入力DataFrameの検証
        required_columns = ['顧客ID', '来店日', 'ステータス', 
                            'スタイリスト名', '予約時HotPepperBeautyクーポン', 
                            '予約時合計金額', '性別', '予約時メニュー'] # '年代' はオプションとする
        for col in required_columns:
            if col not in df.columns:
                logger.error(f"入力DataFrameに必要なカラム '{col}' が存在しません。")
                raise ValueError(f"入力DataFrameに必要なカラム '{col}' が存在しません。")
        
        if not pd.api.types.is_datetime64_any_dtype(df['来店日']):
            logger.error("入力DataFrameのカラム '来店日' はdatetime型である必要があります。")
            raise ValueError("入力DataFrameのカラム '来店日' はdatetime型である必要があります。")

        # 日付パラメータの検証
        date_params = {
            "new_customer_start": new_customer_start,
            "new_customer_end": new_customer_end,
            "repeat_analysis_end": repeat_analysis_end
        }
        for param_name, date_str in date_params.items():
            try:
                pd.to_datetime(date_str) # datetimeに変換可能かチェック
            except ValueError:
                logger.error(f"日付パラメータ '{param_name}' の形式が不正です: {date_str}。YYYY-MM-DD形式で指定してください。")
                raise ValueError(f"日付パラメータ '{param_name}' の形式が不正です: {date_str}。YYYY-MM-DD形式で指定してください。")

        if pd.to_datetime(new_customer_start) > pd.to_datetime(new_customer_end):
            logger.error("新規顧客抽出開始日は新規顧客抽出終了日より前の日付である必要があります。")
            raise ValueError("新規顧客抽出開始日は新規顧客抽出終了日より前の日付である必要があります。")
        if pd.to_datetime(new_customer_end) > pd.to_datetime(repeat_analysis_end):
            logger.error("新規顧客抽出終了日はリピート集計終了日より前の日付である必要があります。") # 通常は同日か前
            # このチェックは要件によるが、一般的にはリピート集計は新規抽出期間の後なので警告に留めるか、より厳密なロジックが必要か検討
            # raise ValueError("新規顧客抽出終了日はリピート集計終了日より前の日付である必要があります。")


        if target_rates is None:
            target_rates = {
                'first_repeat': 35.0,
                'second_repeat': 45.0, 
                'third_repeat': 60.0
            }
        
        # 新規顧客抽出
        # DataProcessor.get_new_customers は '顧客ID' と '来店日' (初回来店日) を含む new_customers DataFrame を返す想定
        new_customers = self.data_processor.get_new_customers(
            df, new_customer_start, new_customer_end
        )
        
        if len(new_customers) == 0:
            logger.warning("指定期間に新規顧客が見つかりませんでした。分析結果は空になります。")
            # 空の結果を返すか、エラーとするかは仕様による。ここでは空の結果を許容する。
            # raise ValueError("指定期間に新規顧客が見つかりません") 
            # 空の分析結果を返すための処理
            empty_analysis_results = {
                'basic_stats': {}, 'funnel_analysis': {}, 'stylist_analysis': {},
                'coupon_analysis': {}, 'target_comparison': {}, 'period_analysis': {},
                'monthly_analysis': {},
                'parameters': {
                    'new_customer_start': new_customer_start, 'new_customer_end': new_customer_end,
                    'repeat_analysis_end': repeat_analysis_end, 'min_repeat_count': min_repeat_count,
                    'min_stylist_customers': min_stylist_customers, 'min_coupon_customers': min_coupon_customers,
                    'target_rates': target_rates, 'error': '指定期間に新規顧客が見つかりません'
                }
            }
            return empty_analysis_results

        
        # リピート状況分析
        # _analyze_repeat_patterns は new_customers から '顧客ID' と '来店日' (初回来店日) を使用する
        repeat_data = self._analyze_repeat_patterns(
            df, new_customers, repeat_analysis_end
        )
        
        # 各種分析実行
        results = {
            'basic_stats': self._calculate_basic_stats(repeat_data, min_repeat_count),
            'funnel_analysis': self._analyze_repeat_funnel(repeat_data),
            'stylist_analysis': self._analyze_by_stylist(repeat_data, min_stylist_customers, min_repeat_count),
            'coupon_analysis': self._analyze_by_coupon(repeat_data, min_coupon_customers, min_repeat_count),
            'target_comparison': self._compare_with_targets(repeat_data, target_rates),
            'period_analysis': self._analyze_repeat_periods(repeat_data),
            'monthly_analysis': self._analyze_monthly_trends(new_customers, repeat_data),
            'parameters': {
                'new_customer_start': new_customer_start,
                'new_customer_end': new_customer_end,
                'repeat_analysis_end': repeat_analysis_end,
                'min_repeat_count': min_repeat_count,
                'min_stylist_customers': min_stylist_customers,
                'min_coupon_customers': min_coupon_customers,
                'target_rates': target_rates
            }
        }
        
        logger.info("リピート分析完了")
        return results
    
    def _analyze_repeat_patterns(self, 
                               all_data: pd.DataFrame,
                               new_customers: pd.DataFrame, 
                               analysis_end_date: str) -> pd.DataFrame:
        """
        新規顧客のリピートパターンを分析 (効率化版)
        
        Args:
            all_data: 全来店データ (必要なカラム: 顧客ID, 来店日, ステータス, etc.)
            new_customers: 新規顧客データ (DataProcessor.get_new_customers の出力。顧客ID, 来店日 を含む)
            analysis_end_date: 分析終了日 (YYYY-MM-DD形式の文字列)
            
        Returns:
            リピートデータ (顧客ID, 初回来店日, リピート回数, リピート日付リスト, 初回リピート日数, etc.)
        """
        logger.info("リピートパターン分析開始（効率化版）")
        
        if '顧客ID' not in all_data.columns or '来店日' not in all_data.columns:
            logger.error("all_dataに必要なカラム（顧客ID, 来店日）が含まれていません。")
            raise ValueError("all_dataに必要なカラム（顧客ID, 来店日）が含まれていません。")
        
        # new_customers DataFrame の必須カラムチェック: '来店日' を使用
        required_nc_cols = ['顧客ID', '来店日', 'スタイリスト名', 
                            '予約時HotPepperBeautyクーポン', '性別', 
                            '予約時メニュー', '予約時合計金額'] # '年代' はオプション
        
        for col in required_nc_cols:
            if col not in new_customers.columns:
                # '年代' はオプションなので、存在しない場合は後で '不明' として処理されるため、エラーにしない
                if col == '年代' and col not in new_customers.columns: 
                    continue 
                logger.error(f"new_customersに必要なカラム '{col}' が含まれていません。")
                raise ValueError(f"new_customersに必要なカラム '{col}' が含まれていません。")

        # analysis_end_date を datetime オブジェクトに変換
        try:
            end_dt = pd.to_datetime(analysis_end_date)
        except ValueError as e:
            logger.error(f"analysis_end_date の形式が不正です: {analysis_end_date} - {e}")
            raise ValueError(f"analysis_end_date の形式が不正です: {analysis_end_date}")

        # new_customers から必要な情報を抽出・リネーム
        # '来店日' は初回来店日を指す
        nc_cols_to_select = ['顧客ID', '来店日', 'スタイリスト名', 
                             '予約時HotPepperBeautyクーポン', '性別', 
                             '予約時メニュー', '予約時合計金額']
        if '年代' in new_customers.columns: # 年代カラムが存在すれば含める
            nc_cols_to_select.append('年代')

        nc_df = new_customers[nc_cols_to_select].copy()
        
        nc_df.rename(columns={'来店日': '初回来店日', 
                              '予約時HotPepperBeautyクーポン': '初回クーポン',
                              '予約時メニュー': '初回メニュー',
                              '予約時合計金額': '初回金額'}, inplace=True)
        
        # 年代カラムの処理 (nc_cols_to_select で '年代' が含まれていれば既にコピーされている)
        # rename後も '年代' のままなので、もし '年代' がなかった場合は '不明' で作成
        if '年代' not in nc_df.columns: # new_customers に '年代' がなかった場合
            nc_df['年代'] = '不明'


        # 全データから新規顧客の来店記録のみを対象とする
        relevant_visits = all_data[all_data['顧客ID'].isin(nc_df['顧客ID'])].copy()
        
        # 来店日をdatetime型に変換 (エラーチェック済みのはずだが念のため)
        relevant_visits['来店日'] = pd.to_datetime(relevant_visits['来店日'])
        nc_df['初回来店日'] = pd.to_datetime(nc_df['初回来店日'])

        # リピート訪問の定義：
        # 1. 新規顧客の初回来店日より後の訪問
        # 2. 分析終了日以前の訪問
        # 3. ステータスが '済み'
        
        # relevant_visits と nc_df (初回来店日情報を持つ) をマージ
        merged_visits = pd.merge(relevant_visits, nc_df[['顧客ID', '初回来店日']], on='顧客ID', how='left')
        
        # リピート訪問をフィルタリング
        repeat_visits_df = merged_visits[
            (merged_visits['来店日'] > merged_visits['初回来店日']) &
            (merged_visits['来店日'] <= end_dt) &
            (merged_visits['ステータス'] == '済み')
        ].copy()
        
        if repeat_visits_df.empty:
            logger.info("リピート訪問データがありません。")
            # リピートがない顧客も含めるために、nc_dfをベースに結果を構築
            result_df = nc_df.copy()
            result_df['リピート回数'] = 0
            result_df['リピート日付リスト'] = [[] for _ in range(len(result_df))]
            result_df['初回リピート日数'] = np.nan
            logger.info(f"リピートパターン分析完了（リピートなし）: {len(result_df)}人")
            return result_df.set_index('顧客ID').reset_index() # 念のためインデックスをリセット

        # 来店日でソート
        repeat_visits_df = repeat_visits_df.sort_values(['顧客ID', '来店日'])
        
        # 顧客ごとにリピート情報を集約
        agg_funcs = {
            '来店日': [
                ('リピート日付リスト', lambda x: list(x)),
                ('初回リピート日', 'first'),
                ('リピート回数', 'count')
            ]
        }
        grouped_repeats = repeat_visits_df.groupby('顧客ID').agg(agg_funcs)
        
        # マルチインデックスのカラムをフラット化
        grouped_repeats.columns = ['_'.join(col).strip('_') for col in grouped_repeats.columns.values]
        grouped_repeats.rename(columns={'来店日_リピート日付リスト': 'リピート日付リスト',
                                      '来店日_初回リピート日': '初回リピート日',
                                      '来店日_リピート回数': 'リピート回数'}, inplace=True)
        grouped_repeats.reset_index(inplace=True)

        # nc_df と grouped_repeats をマージして、全新規顧客にリピート情報を付与
        # リピートがない顧客は NaN になるので、後で処理
        result_df = pd.merge(nc_df, grouped_repeats, on='顧客ID', how='left')
        
        # リピートがない顧客の 'リピート回数' を 0 に、'リピート日付リスト' を空リストに
        result_df['リピート回数'].fillna(0, inplace=True)
        result_df['リピート回数'] = result_df['リピート回数'].astype(int) # 整数型に
        # result_df['リピート日付リスト'].fillna([[] for _ in range(len(result_df))], inplace=True) # fillna に list は使えない
        result_df['リピート日付リスト'] = result_df['リピート日付リスト'].apply(lambda x: x if isinstance(x, list) else [])

        # 初回リピート日数を計算
        result_df['初回リピート日数'] = (result_df['初回リピート日'] - result_df['初回来店日']).dt.days
        # リピートがない顧客の 初回リピート日、初回リピート日数 は NaT/NaN のまま
        
        logger.info(f"リピートパターン分析完了: {len(result_df)}人")
        
        # 不要なカラムを削除 (初回リピート日 は日数計算後不要)
        if '初回リピート日' in result_df.columns:
             result_df.drop(columns=['初回リピート日'], inplace=True)

        # 顧客IDを一度インデックスにしてからリセットすることで、カラム順を整理しつつ顧客IDを先頭に
        result_df = result_df.set_index('顧客ID').reset_index()
        
        # 想定されるカラム順に並び替え (任意、可読性のため)
        # '顧客ID', '初回来店日', 'リピート回数', 'リピート日付リスト', '初回リピート日数', 
        # 'スタイリスト名', '初回クーポン', '性別', '年代', '初回メニュー', '初回金額'
        expected_columns = ['顧客ID', '初回来店日', 'スタイリスト名', '初回クーポン', '性別', '年代', '初回メニュー', '初回金額',
                            'リピート回数', 'リピート日付リスト', '初回リピート日数']
        # 存在するカラムのみで並び替え
        final_columns = [col for col in expected_columns if col in result_df.columns]
        # 存在しないカラムがexpected_columnsにある場合、それ以外のカラムも追加
        for col in result_df.columns:
            if col not in final_columns:
                final_columns.append(col)

        return result_df[final_columns]
    
    def _calculate_age_group(self, customer_row) -> str:
        """年代を計算（簡易版）"""
        # new_customers DataFrame に '年代' カラムが追加される前提。
        # ここでは使用されず、_analyze_repeat_patterns内で直接 '年代' を扱う。
        # 将来的に customer_row を使う複雑な年代計算が必要な場合はここに実装。
        if '年代' in customer_row and pd.notna(customer_row['年代']):
            return customer_row['年代']
        return "不明"
    
    def _calculate_basic_stats(self, repeat_df: pd.DataFrame, min_repeat_count: int) -> Dict:
        """基本統計の計算"""
        total_new_customers = len(repeat_df)
        
        # X回以上リピート
        x_plus_repeaters = len(repeat_df[repeat_df['リピート回数'] >= min_repeat_count])
        x_plus_rate = (x_plus_repeaters / total_new_customers * 100) if total_new_customers > 0 else 0
        
        # 初回リピート
        first_repeaters = len(repeat_df[repeat_df['リピート回数'] >= 1])
        first_repeat_rate = (first_repeaters / total_new_customers * 100) if total_new_customers > 0 else 0
        
        # 平均リピート回数
        avg_repeat_all = repeat_df['リピート回数'].mean()
        avg_repeat_repeaters = repeat_df[repeat_df['リピート回数'] > 0]['リピート回数'].mean()
        
        return {
            'total_new_customers': total_new_customers,
            'x_plus_repeaters': x_plus_repeaters,
            'x_plus_rate': round(x_plus_rate, 1),
            'first_repeaters': first_repeaters,
            'first_repeat_rate': round(first_repeat_rate, 1),
            'avg_repeat_all': round(avg_repeat_all, 1),
            'avg_repeat_repeaters': round(avg_repeat_repeaters, 1) if not pd.isna(avg_repeat_repeaters) else 0,
            'min_repeat_count': min_repeat_count
        }
    
    def _analyze_repeat_funnel(self, repeat_df: pd.DataFrame) -> Dict:
        """リピートファネル分析"""
        total_customers = len(repeat_df)
        
        # 各ステージの顧客数
        stages = {
            '新規来店': total_customers,
            '2回目来店': len(repeat_df[repeat_df['リピート回数'] >= 1]),
            '3回目来店': len(repeat_df[repeat_df['リピート回数'] >= 2]),
            '4回目来店': len(repeat_df[repeat_df['リピート回数'] >= 3]),
            '5回目来店': len(repeat_df[repeat_df['リピート回数'] >= 4])
        }
        
        # 継続率計算
        continuation_rates = {}
        stage_list = list(stages.keys())
        
        for i in range(1, len(stage_list)):
            prev_stage = stage_list[i-1]
            curr_stage = stage_list[i]
            
            if stages[prev_stage] > 0:
                rate = (stages[curr_stage] / stages[prev_stage]) * 100
                continuation_rates[curr_stage] = round(rate, 1)
            else:
                continuation_rates[curr_stage] = 0
        
        # 各ステージの割合（新規顧客総数比）
        stage_rates = {}
        for stage, count in stages.items():
            stage_rates[stage] = round((count / total_customers * 100), 1) if total_customers > 0 else 0
        
        # リピート回数分布
        repeat_distribution = repeat_df['リピート回数'].value_counts().sort_index()
        
        # 累積割合
        cumulative_percentages = {}
        cumulative_count = 0
        for repeat_count in sorted(repeat_distribution.index):
            cumulative_count += repeat_distribution[repeat_count]
            cumulative_percentages[repeat_count] = round((cumulative_count / total_customers * 100), 1)
        
        return {
            'stages': stages,
            'stage_rates': stage_rates,
            'continuation_rates': continuation_rates,
            'repeat_distribution': repeat_distribution.to_dict(),
            'cumulative_percentages': cumulative_percentages
        }
    
    def _analyze_by_stylist(self, repeat_df: pd.DataFrame, min_customers: int, min_repeat_count: int) -> Dict:
        """スタイリスト別分析"""
        stylist_stats = []
        
        # スタイリスト名の前処理: _analyze_repeat_patterns で 'スタイリスト名' が付与されている前提
        if 'スタイリスト名' not in repeat_df.columns:
            logger.warning("スタイリスト別分析: repeat_dfにスタイリスト名カラムがありません。")
            return {"error": "スタイリスト名カラムがありません"}

        repeat_df['スタイリスト名_clean'] = repeat_df['スタイリスト名'].fillna('不明').replace('', '不明')
        
        stylist_groups = repeat_df.groupby('スタイリスト名_clean')
        
        for stylist_name, group in stylist_groups:
            total_customers = len(group)
            
            # 最低顧客数フィルタ
            if total_customers < min_customers:
                continue
            
            x_plus_repeaters = len(group[group['リピート回数'] >= min_repeat_count])
            x_plus_rate = (x_plus_repeaters / total_customers * 100) if total_customers > 0 else 0
            
            first_repeaters = len(group[group['リピート回数'] >= 1])
            first_repeat_rate = (first_repeaters / total_customers * 100) if total_customers > 0 else 0
            
            avg_repeat = group['リピート回数'].mean()
            
            stylist_stats.append({
                'stylist_name': stylist_name,
                'total_customers': total_customers,
                'x_plus_repeaters': x_plus_repeaters,
                'x_plus_rate': round(x_plus_rate, 1),
                'first_repeaters': first_repeaters,
                'first_repeat_rate': round(first_repeat_rate, 1),
                'avg_repeat': round(avg_repeat, 1)
            })
        
        # X回以上リピート率でソート
        stylist_stats.sort(key=lambda x: x['x_plus_rate'], reverse=True)
        
        # トップスタイリスト
        top_stylist = stylist_stats[0] if stylist_stats else None
        top_stylist_info = {
            'name': top_stylist['stylist_name'] if top_stylist else '該当なし',
            'rate': top_stylist['x_plus_rate'] if top_stylist else 0
        }
        
        # 全体でのX回以上リピーター数
        total_x_plus = len(repeat_df[repeat_df['リピート回数'] >= min_repeat_count])
        
        return {
            'stylist_stats': stylist_stats,
            'top_stylist': top_stylist_info,
            'total_x_plus_repeaters': total_x_plus,
            'min_customers_filter': min_customers
        }
    
    def _analyze_by_coupon(self, repeat_df: pd.DataFrame, min_customers: int, min_repeat_count: int) -> Dict:
        """クーポン別分析"""
        coupon_stats = []

        # クーポン名の前処理: _analyze_repeat_patterns で '初回クーポン' が付与されている前提
        if '初回クーポン' not in repeat_df.columns:
            logger.warning("クーポン別分析: repeat_dfに初回クーポンカラムがありません。")
            return {"error": "初回クーポンカラムがありません"}

        repeat_df['初回クーポン_clean'] = repeat_df['初回クーポン'].fillna('なし').replace('', 'なし')
        
        coupon_groups = repeat_df.groupby('初回クーポン_clean')
        
        for coupon_name, group in coupon_groups:
            total_customers = len(group)
            
            # 最低顧客数フィルタ
            if total_customers < min_customers:
                continue
            
            x_plus_repeaters = len(group[group['リピート回数'] >= min_repeat_count])
            x_plus_rate = (x_plus_repeaters / total_customers * 100) if total_customers > 0 else 0
            
            first_repeaters = len(group[group['リピート回数'] >= 1])
            first_repeat_rate = (first_repeaters / total_customers * 100) if total_customers > 0 else 0
            
            # リピーターのみの平均リピート回数
            repeaters_only = group[group['リピート回数'] > 0]
            avg_repeat_repeaters = repeaters_only['リピート回数'].mean() if len(repeaters_only) > 0 else 0
            
            coupon_stats.append({
                'coupon_name': coupon_name,
                'total_customers': total_customers,
                'x_plus_repeaters': x_plus_repeaters,
                'x_plus_rate': round(x_plus_rate, 1),
                'first_repeaters': first_repeaters,
                'first_repeat_rate': round(first_repeat_rate, 1),
                'avg_repeat_repeaters': round(avg_repeat_repeaters, 1)
            })
        
        # X回以上リピート率でソート
        coupon_stats.sort(key=lambda x: x['x_plus_rate'], reverse=True)
        
        # 最も効果的なクーポン
        best_coupon = coupon_stats[0] if coupon_stats else None
        best_coupon_info = {
            'name': best_coupon['coupon_name'] if best_coupon else '該当なし',
            'rate': best_coupon['x_plus_rate'] if best_coupon else 0,
            'avg_repeat': best_coupon['avg_repeat_repeaters'] if best_coupon else 0
        }
        
        return {
            'coupon_stats': coupon_stats,
            'best_coupon': best_coupon_info,
            'min_customers_filter': min_customers
        }
    
    def _compare_with_targets(self, repeat_df: pd.DataFrame, target_rates: Dict[str, float]) -> Dict:
        """目標値比較分析"""
        total_customers = len(repeat_df)
        
        # 実績値計算
        actual_rates = {
            'first_repeat': len(repeat_df[repeat_df['リピート回数'] >= 1]) / total_customers * 100 if total_customers > 0 else 0,
            'second_repeat': 0,  # 3回目来店/2回目来店の計算が必要
            'third_repeat': 0    # 4回目来店/3回目来店の計算が必要
        }
        
        # 継続率の正確な計算
        second_visit_customers = len(repeat_df[repeat_df['リピート回数'] >= 1])
        third_visit_customers = len(repeat_df[repeat_df['リピート回数'] >= 2])
        fourth_visit_customers = len(repeat_df[repeat_df['リピート回数'] >= 3])
        
        if second_visit_customers > 0:
            actual_rates['second_repeat'] = (third_visit_customers / second_visit_customers) * 100
        
        if third_visit_customers > 0:
            actual_rates['third_repeat'] = (fourth_visit_customers / third_visit_customers) * 100
        
        # 目標達成率
        achievement_rates = {}
        for stage, actual in actual_rates.items():
            target = target_rates.get(stage, 0)
            achievement_rates[stage] = (actual / target * 100) if target > 0 else 0
        
        # 全体目標達成率
        overall_achievement = sum(achievement_rates.values()) / len(achievement_rates)
        
        # 必要な追加顧客数計算
        required_additional = {}
        current_counts = {
            'first_repeat': len(repeat_df[repeat_df['リピート回数'] >= 1]),
            'second_repeat': third_visit_customers,
            'third_repeat': fourth_visit_customers
        }
        
        base_counts = {
            'first_repeat': total_customers,
            'second_repeat': second_visit_customers,
            'third_repeat': third_visit_customers
        }
        
        for stage, target_rate in target_rates.items():
            base_count = base_counts.get(stage, 0)
            current_count = current_counts.get(stage, 0)
            target_count = base_count * (target_rate / 100)
            additional_needed = max(0, target_count - current_count)
            
            required_additional[stage] = {
                'target_count': round(target_count),
                'current_count': current_count,
                'additional_needed': round(additional_needed)
            }
        
        return {
            'target_rates': target_rates,
            'actual_rates': {k: round(v, 1) for k, v in actual_rates.items()},
            'achievement_rates': {k: round(v, 1) for k, v in achievement_rates.items()},
            'overall_achievement': round(overall_achievement, 1),
            'required_additional': required_additional
        }
    
    def _analyze_repeat_periods(self, repeat_df: pd.DataFrame) -> Dict:
        """リピートまでの期間分析"""
        if '初回リピート日数' not in repeat_df.columns:
            logger.warning("初回リピート日数が計算されていないため、期間分析をスキップします。")
            return {
                'avg_days': 0, 'median_days': 0, 'min_days': 0, 'max_days': 0,
                'period_distribution': {}
            }

        # NaNを除外したコピーで計算（元データに影響を与えないように）
        repeat_customers_with_days = repeat_df[repeat_df['初回リピート日数'].notna()].copy()

        if repeat_customers_with_days.empty:
            logger.info("リピート顧客が存在しないか、初回リピート日数がすべてNaNです。")
            return {
                'avg_days': 0, 'median_days': 0, 'min_days': 0, 'max_days': 0,
                'period_distribution': {}
            }

        avg_days = repeat_customers_with_days['初回リピート日数'].mean()
        median_days = repeat_customers_with_days['初回リピート日数'].median()
        min_days = repeat_customers_with_days['初回リピート日数'].min()
        max_days = repeat_customers_with_days['初回リピート日数'].max()

        def categorize_period(days_val):
            if pd.isna(days_val):
                return '不明'
            if days_val <= 7:
                return '1週間以内'
            elif days_val <= 14:
                return '2週間以内'
            elif days_val <= 30:
                return '1ヶ月以内'
            elif days_val <= 60:
                return '2ヶ月以内'
            elif days_val <= 90:
                return '3ヶ月以内'
            else:
                return '3ヶ月以上'

        # .locを使用してSettingWithCopyWarningを回避
        repeat_customers_with_days.loc[:, '期間区分'] = repeat_customers_with_days['初回リピート日数'].apply(categorize_period)
        
        period_distribution = repeat_customers_with_days['期間区分'].value_counts(normalize=True) * 100
        period_distribution_counts = repeat_customers_with_days['期間区分'].value_counts()

        distribution_dict = {
            cat: {'count': int(period_distribution_counts.get(cat, 0)), 
                  'percentage': round(float(period_distribution.get(cat, 0.0)), 1)}
            for cat in period_distribution.index
        }
        # 期間区分の順序を定義
        defined_order = ['1週間以内', '2週間以内', '1ヶ月以内', '2ヶ月以内', '3ヶ月以内', '3ヶ月以上', '不明']
        sorted_distribution = {cat: distribution_dict[cat] for cat in defined_order if cat in distribution_dict}

        return {
            'avg_days': round(float(avg_days), 1) if pd.notna(avg_days) else 0.0,
            'median_days': round(float(median_days), 1) if pd.notna(median_days) else 0.0,
            'min_days': int(min_days) if pd.notna(min_days) else 0,
            'max_days': int(max_days) if pd.notna(max_days) else 0,
            'period_distribution': sorted_distribution
        }
    
    def _analyze_monthly_trends(self, new_customers: pd.DataFrame, repeat_df: pd.DataFrame) -> Dict:
        """月別トレンド分析"""
        # 新規顧客の月別集計
        new_customers['年月'] = new_customers['来店日'].dt.to_period('M')
        monthly_new = new_customers.groupby('年月').size()
        
        # リピートデータに年月を追加
        repeat_with_month = repeat_df.merge(
            new_customers[['顧客ID', '年月']],
            on='顧客ID',
            how='left'
        )
        
        # 月別初回リピート率
        monthly_repeat_rates = {}
        for month, group in repeat_with_month.groupby('年月'):
            total = len(group)
            repeaters = len(group[group['リピート回数'] >= 1])
            rate = (repeaters / total * 100) if total > 0 else 0
            
            monthly_repeat_rates[str(month)] = {
                'new_customers': total,
                'repeaters': repeaters,
                'repeat_rate': round(rate, 1)
            }
        
        return {
            'monthly_new_customers': {str(k): v for k, v in monthly_new.items()},
            'monthly_repeat_rates': monthly_repeat_rates
        } 