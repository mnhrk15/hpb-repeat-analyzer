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
            new_customer_start: 新規顧客抽出開始日
            new_customer_end: 新規顧客抽出終了日
            repeat_analysis_end: リピート集計終了日
            min_repeat_count: X回以上リピートの基準回数
            min_stylist_customers: スタイリスト分析の最低顧客数
            min_coupon_customers: クーポン分析の最低顧客数
            target_rates: 目標値辞書
            
        Returns:
            分析結果辞書
        """
        logger.info("リピート分析開始")
        
        if target_rates is None:
            target_rates = {
                'first_repeat': 35.0,
                'second_repeat': 45.0, 
                'third_repeat': 60.0
            }
        
        # 新規顧客抽出
        new_customers = self.data_processor.get_new_customers(
            df, new_customer_start, new_customer_end
        )
        
        if len(new_customers) == 0:
            raise ValueError("指定期間に新規顧客が見つかりません")
        
        # リピート状況分析
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
        新規顧客のリピートパターンを分析
        
        Args:
            all_data: 全来店データ
            new_customers: 新規顧客データ
            analysis_end_date: 分析終了日
            
        Returns:
            リピートデータ
        """
        end_dt = datetime.strptime(analysis_end_date, '%Y-%m-%d')
        
        repeat_results = []
        
        for _, new_customer in new_customers.iterrows():
            customer_id = new_customer['顧客ID']
            first_visit_date = new_customer['来店日_parsed']
            
            # この顧客の全来店履歴（初回以降）
            customer_visits = all_data[
                (all_data['顧客ID'] == customer_id) &
                (all_data['来店日_parsed'] > first_visit_date) &
                (all_data['来店日_parsed'] <= end_dt) &
                (all_data['ステータス'] == '済み')
            ].copy()
            
            # 来店日でソート
            customer_visits = customer_visits.sort_values('来店日_parsed')
            
            # リピート回数と日付情報
            repeat_count = len(customer_visits)
            repeat_dates = customer_visits['来店日_parsed'].tolist()
            
            # 初回リピートまでの日数
            first_repeat_days = None
            if repeat_count > 0:
                first_repeat_days = (repeat_dates[0] - first_visit_date).days
            
            # 新規顧客情報とリピート情報を結合
            repeat_info = {
                '顧客ID': customer_id,
                '初回来店日': first_visit_date,
                'リピート回数': repeat_count,
                'リピート日付リスト': repeat_dates,
                '初回リピート日数': first_repeat_days,
                'スタイリスト名': new_customer.get('スタイリスト名', '不明'),
                '予約時HotPepperBeautyクーポン': new_customer.get('予約時HotPepperBeautyクーポン', ''),
                '性別': new_customer.get('性別', ''),
                '年代': self._calculate_age_group(new_customer),
                '初回メニュー': new_customer.get('予約時メニュー', ''),
                '初回金額': new_customer.get('予約時合計金額', 0)
            }
            
            repeat_results.append(repeat_info)
        
        repeat_df = pd.DataFrame(repeat_results)
        logger.info(f"リピートパターン分析完了: {len(repeat_df)}人")
        
        return repeat_df
    
    def _calculate_age_group(self, customer_row) -> str:
        """年代を計算（簡易版）"""
        # 実装が複雑になるため、仮の処理
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
        
        # スタイリスト名の前処理
        repeat_df['スタイリスト名_clean'] = repeat_df['スタイリスト名'].fillna('不明')
        repeat_df['スタイリスト名_clean'] = repeat_df['スタイリスト名_clean'].replace('', '不明')
        
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
        
        # クーポン名の前処理
        repeat_df['クーポン_clean'] = repeat_df['予約時HotPepperBeautyクーポン'].fillna('クーポンなし')
        repeat_df['クーポン_clean'] = repeat_df['クーポン_clean'].replace('', 'クーポンなし')
        
        coupon_groups = repeat_df.groupby('クーポン_clean')
        
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
        repeat_customers = repeat_df[repeat_df['初回リピート日数'].notna()]
        
        if len(repeat_customers) == 0:
            return {
                'avg_days': 0,
                'median_days': 0,
                'min_days': 0,
                'max_days': 0,
                'period_distribution': {}
            }
        
        days = repeat_customers['初回リピート日数']
        
        # 統計値
        stats = {
            'avg_days': round(days.mean(), 1),
            'median_days': round(days.median(), 1),
            'min_days': int(days.min()),
            'max_days': int(days.max())
        }
        
        # 期間区分ごとの分布
        def categorize_period(days_val):
            if days_val <= 30:
                return '30日以内'
            elif days_val <= 60:
                return '31-60日'
            elif days_val <= 90:
                return '61-90日'
            elif days_val <= 180:
                return '91-180日'
            elif days_val <= 365:
                return '181-365日'
            else:
                return '365日超'
        
        repeat_customers['期間区分'] = repeat_customers['初回リピート日数'].apply(categorize_period)
        period_dist = repeat_customers['期間区分'].value_counts()
        
        # 割合計算
        total_repeaters = len(repeat_customers)
        period_distribution = {}
        for period, count in period_dist.items():
            percentage = (count / total_repeaters * 100) if total_repeaters > 0 else 0
            period_distribution[period] = {
                'count': count,
                'percentage': round(percentage, 1)
            }
        
        return {
            **stats,
            'period_distribution': period_distribution
        }
    
    def _analyze_monthly_trends(self, new_customers: pd.DataFrame, repeat_df: pd.DataFrame) -> Dict:
        """月別トレンド分析"""
        # 新規顧客の月別集計
        new_customers['年月'] = new_customers['来店日_parsed'].dt.to_period('M')
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