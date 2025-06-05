"""
レポート生成モジュール - テキストレポート・Excel出力
"""

import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)

class ReportGenerator:
    """レポート生成クラス"""
    
    def __init__(self):
        self.reports_dir = 'reports'
        os.makedirs(self.reports_dir, exist_ok=True)
    
    def generate_text_report(self, analysis_results: Dict, parameters: Dict) -> str:
        """
        テキストレポートを生成
        
        Args:
            analysis_results: 分析結果辞書
            parameters: 分析パラメータ
            
        Returns:
            生成されたレポートファイルのパス
        """
        logger.info("テキストレポート生成開始")
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"リピート分析レポート_{timestamp}.txt"
        filepath = os.path.join(self.reports_dir, filename)
        
        report_content = self._create_text_content(analysis_results, parameters)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"テキストレポート生成完了: {filepath}")
        return filepath
    
    def _create_header_section(self, params: Dict) -> List[str]:
        content = []
        content.append("=" * 80)
        content.append("美容室顧客データリピート分析レポート")
        content.append("=" * 80)
        content.append("")
        content.append("【分析条件】")
        content.append(f"新規顧客抽出期間: {params.get('new_customer_start', '不明')} ～ {params.get('new_customer_end', '不明')}")
        content.append(f"リピート集計終了日: {params.get('repeat_analysis_end', '不明')}")
        content.append(f"X回以上リピート基準: {params.get('min_repeat_count', '不明')}回")
        content.append("")
        return content

    def _create_basic_stats_section(self, basic_stats: Dict, params: Dict) -> List[str]:
        content = []
        content.append("【基本統計】")
        content.append(f"新規顧客総数: {basic_stats.get('total_new_customers', 0):,}人")
        content.append(f"{params.get('min_repeat_count', 'X')}回以上リピーター数: {basic_stats.get('x_plus_repeaters', 0):,}人")
        content.append(f"{params.get('min_repeat_count', 'X')}回以上リピート率: {basic_stats.get('x_plus_rate', 0.0):.1f}%")
        content.append(f"初回リピート率: {basic_stats.get('first_repeat_rate', 0.0):.1f}%")
        content.append(f"平均リピート回数（全顧客）: {basic_stats.get('avg_repeat_all', 0.0):.1f}回")
        content.append(f"平均リピート回数（リピーターのみ）: {basic_stats.get('avg_repeat_repeaters', 0.0):.1f}回")
        content.append("")
        return content

    def _create_funnel_analysis_section(self, funnel_analysis: Dict) -> List[str]:
        content = []
        content.append("【リピートファネル分析】")
        content.append("■ステージ別顧客数")
        stages_data = funnel_analysis.get('stages', {})
        stage_rates_data = funnel_analysis.get('stage_rates', {})
        if stages_data:
            for stage, count in stages_data.items():
                rate = stage_rates_data.get(stage, 0.0)
                content.append(f"  {stage}: {count:,}人 ({rate:.1f}%)")
        else:
            content.append("  データがありません。")
        
        content.append("")
        content.append("■ステージ間継続率")
        continuation_rates_data = funnel_analysis.get('continuation_rates', {})
        if continuation_rates_data:
            for stage, rate in continuation_rates_data.items():
                content.append(f"  {stage}: {rate:.1f}%")
        else:
            content.append("  データがありません。")
            
        content.append("")
        content.append("■リピート回数分布")
        repeat_distribution_data = funnel_analysis.get('repeat_distribution', {})
        cumulative_percentages_data = funnel_analysis.get('cumulative_percentages', {})
        if repeat_distribution_data:
            for count_val, customers in sorted(repeat_distribution_data.items()):
                cumulative = cumulative_percentages_data.get(count_val, 0.0)
                content.append(f"  {count_val}回: {customers:,}人 (累積: {cumulative:.1f}%)")
        else:
            content.append("  データがありません。")
        content.append("")
        return content

    def _create_stylist_analysis_section(self, stylist_analysis: Dict, params: Dict) -> List[str]:
        content = []
        content.append("【スタイリスト別分析】")
        min_stylist_customers_param = params.get('min_stylist_customers', 'N/A')
        content.append(f"■サマリー（新規顧客{min_stylist_customers_param}人以上のスタイリストが対象）")
        top_stylist_info = stylist_analysis.get('top_stylist', {})
        top_stylist_name = top_stylist_info.get('name', '不明')
        top_stylist_rate = top_stylist_info.get('rate', 0.0)
        content.append(f"トップスタイリスト: {top_stylist_name} ({top_stylist_rate:.1f}%)")
        min_repeat_count_param = params.get('min_repeat_count', 'X')
        content.append(f"全体{min_repeat_count_param}回以上リピーター数: {stylist_analysis.get('total_x_plus_repeaters', 0):,}人")
        content.append("")
        
        stylist_stats_list = stylist_analysis.get('stylist_stats', [])
        if stylist_stats_list:
            content.append("■スタイリスト別詳細")
            for s_info in stylist_stats_list[:10]: # 上位10名
                s_name = s_info.get('stylist_name', '不明')
                s_x_plus_rate = s_info.get('x_plus_rate', 0.0)
                s_x_plus_repeaters = s_info.get('x_plus_repeaters', 0)
                s_total_customers = s_info.get('total_customers', 0)
                content.append(f"  {s_name}: {s_x_plus_rate:.1f}% ({s_x_plus_repeaters}/{s_total_customers}人)")
        else:
            content.append("  スタイリスト別詳細データがありません。")
        content.append("")
        return content

    def _create_coupon_analysis_section(self, coupon_analysis: Dict, params: Dict) -> List[str]:
        content = []
        content.append("【クーポン別分析】")
        min_coupon_customers_param = params.get('min_coupon_customers', 'N/A')
        min_repeat_count_param = params.get('min_repeat_count', 'X') # basic_statsから取らないように修正
        content.append(f"■サマリー（利用顧客{min_coupon_customers_param}人以上のクーポンが対象）")
        best_coupon_info = coupon_analysis.get('best_coupon', {})
        best_coupon_name = best_coupon_info.get('name', '不明')
        best_coupon_rate = best_coupon_info.get('rate', 0.0)
        best_coupon_avg_repeat = best_coupon_info.get('avg_repeat', 0.0)
        content.append(f"最も効果的なクーポン: {best_coupon_name}")
        content.append(f"  - {min_repeat_count_param}回以上リピート率: {best_coupon_rate:.1f}%")
        content.append(f"  - 平均リピート回数: {best_coupon_avg_repeat:.1f}回")
        content.append("")
        
        coupon_stats_list = coupon_analysis.get('coupon_stats', [])
        if coupon_stats_list:
            content.append("■クーポン別詳細")
            for c_info in coupon_stats_list[:10]:  # 上位10件
                c_name = c_info.get('coupon_name', '不明')
                c_x_plus_rate = c_info.get('x_plus_rate', 0.0)
                c_avg_repeat_repeaters = c_info.get('avg_repeat_repeaters', 0.0)
                content.append(f"  {c_name}: {c_x_plus_rate:.1f}% (平均: {c_avg_repeat_repeaters:.1f}回)")
        else:
            content.append("  クーポン別詳細データがありません。")
        content.append("")
        return content

    def _create_target_comparison_section(self, target_comparison: Dict) -> List[str]:
        content = []
        content.append("【目標値比較分析】")
        content.append(f"全体目標達成率: {target_comparison.get('overall_achievement', 0.0):.1f}%")
        content.append("")
        
        content.append("■目標と実績の比較")
        stages_map = {
            'first_repeat': '初回リピート',
            'second_repeat': '2回目リピート',
            'third_repeat': '3回目リピート'
        }
        target_rates_data = target_comparison.get('target_rates', {})
        actual_rates_data = target_comparison.get('actual_rates', {})
        achievement_rates_data = target_comparison.get('achievement_rates', {})

        if target_rates_data and actual_rates_data and achievement_rates_data:
            for stage_key, stage_name in stages_map.items():
                target_rate = target_rates_data.get(stage_key, 0.0)
                actual_rate = actual_rates_data.get(stage_key, 0.0)
                achievement = achievement_rates_data.get(stage_key, 0.0)
                content.append(f"  {stage_name}: 目標{target_rate:.1f}% vs 実績{actual_rate:.1f}% (達成率: {achievement:.1f}%)")
        else:
            content.append("  目標・実績データが不足しています。")
        content.append("")
        
        content.append("■目標達成に必要な追加顧客数")
        required_additional_data = target_comparison.get('required_additional', {})
        if required_additional_data:
            for stage_key, stage_name in stages_map.items():
                additional_info = required_additional_data.get(stage_key, {})
                additional_needed = additional_info.get('additional_needed', 0)
                current_count = additional_info.get('current_count', 0)
                target_count = additional_info.get('target_count', 0)
                content.append(f"  {stage_name}: {additional_needed}人 (現在: {current_count}人 → 目標: {target_count}人)")
        else:
            content.append("  追加顧客数データが不足しています。")
        content.append("")
        return content

    def _create_period_analysis_section(self, period_analysis: Dict) -> List[str]:
        content = []
        content.append("【リピートまでの期間分析】")
        content.append(f"平均初回リピート日数: {period_analysis.get('avg_days', 0.0):.1f}日")
        content.append(f"中央値初回リピート日数: {period_analysis.get('median_days', 0.0):.1f}日")
        content.append(f"最短初回リピート日数: {period_analysis.get('min_days', 'N/A')}日")
        content.append(f"最長初回リピート日数: {period_analysis.get('max_days', 'N/A')}日")
        content.append("")
        
        period_distribution_data = period_analysis.get('period_distribution', {})
        if period_distribution_data:
            content.append("■期間区分別分布")
            for period_name, data_val in period_distribution_data.items():
                p_count = data_val.get('count', 0)
                p_percentage = data_val.get('percentage', 0.0)
                content.append(f"  {period_name}: {p_count:,}人 ({p_percentage:.1f}%)")
        else:
            content.append("  期間区分別分布データがありません。")
        content.append("")
        return content

    def _create_monthly_trends_section(self, monthly_analysis: Dict) -> List[str]:
        content = []
        content.append("【月別トレンド分析】")
        content.append("■月別新規顧客数")
        monthly_new_customers_data = monthly_analysis.get('monthly_new_customers', {})
        if monthly_new_customers_data:
            for month, count in monthly_new_customers_data.items():
                content.append(f"  {month}: {count:,}人")
        else:
            content.append("  データがありません。")
        content.append("")
        
        content.append("■月別初回リピート率")
        monthly_repeat_rates_data = monthly_analysis.get('monthly_repeat_rates', {})
        if monthly_repeat_rates_data:
            for month, data_val in monthly_repeat_rates_data.items():
                mr_rate = data_val.get('repeat_rate', 0.0)
                mr_repeaters = data_val.get('repeaters', 0)
                mr_new_customers = data_val.get('new_customers', 0)
                content.append(f"  {month}: {mr_rate:.1f}% ({mr_repeaters}/{mr_new_customers}人)")
        else:
            content.append("  データがありません。")
        content.append("")
        return content

    def _create_insights_section(self, results: Dict, params: Dict) -> List[str]:
        content = []
        content.append("【分析コメント・改善提案】")
        insights_text = self._generate_insights(results, params)
        content.append(insights_text if insights_text else "  インサイト生成中に問題が発生したか、対象データがありませんでした。")
        content.append("")
        return content

    def _create_footer_section(self) -> List[str]:
        content = []
        content.append("=" * 80)
        content.append(f"レポート生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
        content.append("=" * 80)
        return content

    def _create_text_content(self, results: Dict, params: Dict) -> str:
        """テキストレポート内容を作成"""
        
        all_content = []
        
        all_content.extend(self._create_header_section(params))
        all_content.extend(self._create_basic_stats_section(results.get('basic_stats', {}), params))
        all_content.extend(self._create_funnel_analysis_section(results.get('funnel_analysis', {})))
        all_content.extend(self._create_stylist_analysis_section(results.get('stylist_analysis', {}), params))
        all_content.extend(self._create_coupon_analysis_section(results.get('coupon_analysis', {}), params))
        all_content.extend(self._create_target_comparison_section(results.get('target_comparison', {})))
        all_content.extend(self._create_period_analysis_section(results.get('period_analysis', {})))
        all_content.extend(self._create_monthly_trends_section(results.get('monthly_analysis', {})))
        all_content.extend(self._create_insights_section(results, params))
        all_content.extend(self._create_footer_section())
        
        return "\n".join(all_content)
    
    def _generate_insights(self, results: Dict, params: Dict) -> str:
        """分析結果に基づくインサイト・提案を生成"""
        insights = []
        
        basic = results.get('basic_stats', {})
        target = results.get('target_comparison', {})
        
        # 全体的な評価
        first_repeat_rate = basic.get('first_repeat_rate', 0.0)
        if isinstance(first_repeat_rate, (int, float)):
            if first_repeat_rate >= 40:
                insights.append("✓ 初回リピート率が良好です。顧客満足度の高いサービス提供ができています。")
            elif first_repeat_rate >= 30:
                insights.append("△ 初回リピート率は平均的です。さらなる向上の余地があります。")
            else:
                insights.append("✗ 初回リピート率が低めです。サービス品質の見直しが必要かもしれません。")
        else:
            insights.append("初回リピート率のデータ形式が不正です。")
        
        # 目標達成状況
        overall_achievement = target.get('overall_achievement', 0.0)
        if isinstance(overall_achievement, (int, float)):
            if overall_achievement >= 80:
                insights.append("✓ 全体的に目標達成率が高く、優秀な成果を上げています。")
            elif overall_achievement >= 60:
                insights.append("△ 目標達成率は中程度です。特定の段階での改善が必要です。")
            else:
                insights.append("✗ 目標達成率が低いです。包括的な改善施策が必要です。")
        else:
            insights.append("全体目標達成率のデータ形式が不正です。")
        
        # 最も改善が必要な段階
        achievement_rates = target.get('achievement_rates', {})
        if achievement_rates and isinstance(achievement_rates, dict) and all(isinstance(v, (int,float)) for v in achievement_rates.values()):
            try:
                worst_stage_key, worst_stage_value = min(achievement_rates.items(), key=lambda x: x[1])
                stage_names_map = {
                    'first_repeat': '初回リピート',
                    'second_repeat': '2回目リピート',
                    'third_repeat': '3回目リピート'
                }
                worst_stage_name = stage_names_map.get(worst_stage_key, worst_stage_key)
                insights.append(f"最も改善が必要な段階: {worst_stage_name} (達成率: {worst_stage_value:.1f}%)")
            except ValueError:
                 insights.append("改善が必要な段階の特定に失敗しました。(データ不足または形式不正)")
        else:
            insights.append("改善が必要な段階のデータが不足しているか、形式が不正です。")
        
        # スタイリスト分析からの示唆
        stylist = results.get('stylist_analysis', {})
        stylist_stats = stylist.get('stylist_stats', [])
        if stylist_stats and isinstance(stylist_stats, list) and len(stylist_stats) > 0:
            try:
                top_stylist_stat = stylist_stats[0]
                if isinstance(top_stylist_stat, dict) and isinstance(top_stylist_stat.get('x_plus_rate'), (int,float)):
                    top_rate = top_stylist_stat['x_plus_rate']
                    if len(stylist_stats) > 1:
                        valid_rates = [s.get('x_plus_rate', 0.0) for s in stylist_stats if isinstance(s, dict) and isinstance(s.get('x_plus_rate'), (int,float))]
                        if valid_rates:
                            avg_rate = sum(valid_rates) / len(valid_rates)
                            if top_rate - avg_rate > 10:
                                insights.append("スタイリスト間での成果にばらつきがあります。"\
                                               "トップスタイリストのノウハウを他スタッフに共有することを推奨します。")
                        else:
                            insights.append("スタイリストの平均リピート率計算に必要なデータが不足しています。")
                else:
                    insights.append("トップスタイリストのリピート率データが不正です。")
            except (IndexError, TypeError, KeyError):
                insights.append("スタイリスト分析からの示唆生成中にエラーが発生しました。")

        # クーポン分析からの示唆
        coupon = results.get('coupon_analysis', {})
        coupon_stats = coupon.get('coupon_stats', [])
        best_coupon_info = coupon.get('best_coupon', {})

        if isinstance(best_coupon_info, dict) and best_coupon_info.get('name'):
            best_coupon_name = best_coupon_info['name']
            insights.append(f"「{best_coupon_name}」が最も効果的なクーポンの可能性があります。"\
                           "このタイプのクーポンの拡充を検討してください。")
        elif coupon_stats:
             insights.append("効果的なクーポンは特定できませんでしたが、クーポン利用データは存在します。")
        else:
            insights.append("クーポン分析に関する十分なデータがありません。")
        
        # 期間分析からの示唆
        period = results.get('period_analysis', {})
        avg_days = period.get('avg_days')
        if isinstance(avg_days, (int, float)) and avg_days > 0:
            if avg_days <= 30:
                insights.append("初回リピートまでの期間が短く、顧客エンゲージメントが高いです。")
            elif avg_days <= 60:
                insights.append("初回リピートまでの期間は標準的です。")
            else:
                insights.append("初回リピートまでの期間がやや長いです。"
                               "フォローアップ施策の強化を検討してください。")
        
        return "\n".join(f"• {insight}" for insight in insights)
    
    def generate_csv_export(self, analysis_results: Dict) -> str:
        """
        分析結果をCSV形式でエクスポート
        
        Args:
            analysis_results: 分析結果辞書
            
        Returns:
            生成されたCSVファイルのパス
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"リピート分析データ_{timestamp}.csv"
        filepath = os.path.join(self.reports_dir, filename)
        
        # 複数のシートに相当するデータを統合してCSV出力
        export_data = []
        
        # 基本統計
        basic = analysis_results['basic_stats']
        export_data.append({
            'カテゴリ': '基本統計',
            '項目': '新規顧客総数',
            '値': basic['total_new_customers'],
            '単位': '人'
        })
        
        # その他のデータも同様に追加...
        
        df = pd.DataFrame(export_data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        logger.info(f"CSV出力完了: {filepath}")
        return filepath 