"""
レポート生成モジュール - テキストレポート・Excel出力
"""

import pandas as pd
from datetime import datetime
import os
import logging
from typing import Dict, Any

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
    
    def _create_text_content(self, results: Dict, params: Dict) -> str:
        """テキストレポート内容を作成"""
        
        content = []
        content.append("=" * 80)
        content.append("美容室顧客データリピート分析レポート")
        content.append("=" * 80)
        content.append("")
        
        # 分析条件
        content.append("【分析条件】")
        content.append(f"新規顧客抽出期間: {params['new_customer_start']} ～ {params['new_customer_end']}")
        content.append(f"リピート集計終了日: {params['repeat_analysis_end']}")
        content.append(f"X回以上リピート基準: {params['min_repeat_count']}回")
        content.append("")
        
        # 基本統計
        basic = results['basic_stats']
        content.append("【基本統計】")
        content.append(f"新規顧客総数: {basic['total_new_customers']:,}人")
        content.append(f"{basic['min_repeat_count']}回以上リピーター数: {basic['x_plus_repeaters']:,}人")
        content.append(f"{basic['min_repeat_count']}回以上リピート率: {basic['x_plus_rate']:.1f}%")
        content.append(f"初回リピート率: {basic['first_repeat_rate']:.1f}%")
        content.append(f"平均リピート回数（全顧客）: {basic['avg_repeat_all']:.1f}回")
        content.append(f"平均リピート回数（リピーターのみ）: {basic['avg_repeat_repeaters']:.1f}回")
        content.append("")
        
        # リピートファネル分析
        funnel = results['funnel_analysis']
        content.append("【リピートファネル分析】")
        content.append("■ステージ別顧客数")
        for stage, count in funnel['stages'].items():
            rate = funnel['stage_rates'][stage]
            content.append(f"  {stage}: {count:,}人 ({rate:.1f}%)")
        
        content.append("")
        content.append("■ステージ間継続率")
        for stage, rate in funnel['continuation_rates'].items():
            content.append(f"  {stage}: {rate:.1f}%")
        
        content.append("")
        content.append("■リピート回数分布")
        for count, customers in sorted(funnel['repeat_distribution'].items()):
            cumulative = funnel['cumulative_percentages'].get(count, 0)
            content.append(f"  {count}回: {customers:,}人 (累積: {cumulative:.1f}%)")
        content.append("")
        
        # スタイリスト別分析
        stylist = results['stylist_analysis']
        content.append("【スタイリスト別分析】")
        content.append(f"■サマリー（新規顧客{params['min_stylist_customers']}人以上のスタイリストが対象）")
        content.append(f"トップスタイリスト: {stylist['top_stylist']['name']} ({stylist['top_stylist']['rate']:.1f}%)")
        content.append(f"全体{basic['min_repeat_count']}回以上リピーター数: {stylist['total_x_plus_repeaters']:,}人")
        content.append("")
        
        if stylist['stylist_stats']:
            content.append("■スタイリスト別詳細")
            for s in stylist['stylist_stats'][:10]:  # 上位10名
                content.append(f"  {s['stylist_name']}: {s['x_plus_rate']:.1f}% "
                             f"({s['x_plus_repeaters']}/{s['total_customers']}人)")
        content.append("")
        
        # クーポン別分析
        coupon = results['coupon_analysis']
        content.append("【クーポン別分析】")
        content.append(f"■サマリー（利用顧客{params['min_coupon_customers']}人以上のクーポンが対象）")
        content.append(f"最も効果的なクーポン: {coupon['best_coupon']['name']}")
        content.append(f"  - {basic['min_repeat_count']}回以上リピート率: {coupon['best_coupon']['rate']:.1f}%")
        content.append(f"  - 平均リピート回数: {coupon['best_coupon']['avg_repeat']:.1f}回")
        content.append("")
        
        if coupon['coupon_stats']:
            content.append("■クーポン別詳細")
            for c in coupon['coupon_stats'][:10]:  # 上位10件
                content.append(f"  {c['coupon_name']}: {c['x_plus_rate']:.1f}% "
                             f"(平均: {c['avg_repeat_repeaters']:.1f}回)")
        content.append("")
        
        # 目標値比較
        target = results['target_comparison']
        content.append("【目標値比較分析】")
        content.append(f"全体目標達成率: {target['overall_achievement']:.1f}%")
        content.append("")
        
        content.append("■目標と実績の比較")
        stages = {
            'first_repeat': '初回リピート',
            'second_repeat': '2回目リピート',
            'third_repeat': '3回目リピート'
        }
        
        for stage_key, stage_name in stages.items():
            target_rate = target['target_rates'][stage_key]
            actual_rate = target['actual_rates'][stage_key]
            achievement = target['achievement_rates'][stage_key]
            content.append(f"  {stage_name}: 目標{target_rate:.1f}% vs 実績{actual_rate:.1f}% "
                         f"(達成率: {achievement:.1f}%)")
        content.append("")
        
        content.append("■目標達成に必要な追加顧客数")
        for stage_key, stage_name in stages.items():
            additional = target['required_additional'][stage_key]
            content.append(f"  {stage_name}: {additional['additional_needed']}人 "
                         f"(現在: {additional['current_count']}人 → 目標: {additional['target_count']}人)")
        content.append("")
        
        # 期間分析
        period = results['period_analysis']
        content.append("【リピートまでの期間分析】")
        content.append(f"平均初回リピート日数: {period['avg_days']:.1f}日")
        content.append(f"中央値初回リピート日数: {period['median_days']:.1f}日")
        content.append(f"最短初回リピート日数: {period['min_days']}日")
        content.append(f"最長初回リピート日数: {period['max_days']}日")
        content.append("")
        
        if period['period_distribution']:
            content.append("■期間区分別分布")
            for period_name, data in period['period_distribution'].items():
                content.append(f"  {period_name}: {data['count']:,}人 ({data['percentage']:.1f}%)")
        content.append("")
        
        # 月別分析
        monthly = results['monthly_analysis']
        content.append("【月別トレンド分析】")
        content.append("■月別新規顧客数")
        for month, count in monthly['monthly_new_customers'].items():
            content.append(f"  {month}: {count:,}人")
        content.append("")
        
        content.append("■月別初回リピート率")
        for month, data in monthly['monthly_repeat_rates'].items():
            content.append(f"  {month}: {data['repeat_rate']:.1f}% "
                         f"({data['repeaters']}/{data['new_customers']}人)")
        content.append("")
        
        # 分析コメント・提案
        content.append("【分析コメント・改善提案】")
        content.append(self._generate_insights(results, params))
        content.append("")
        
        content.append("=" * 80)
        content.append(f"レポート生成日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}")
        content.append("=" * 80)
        
        return "\n".join(content)
    
    def _generate_insights(self, results: Dict, params: Dict) -> str:
        """分析結果に基づくインサイト・提案を生成"""
        insights = []
        
        basic = results['basic_stats']
        target = results['target_comparison']
        funnel = results['funnel_analysis']
        
        # 全体的な評価
        if basic['first_repeat_rate'] >= 40:
            insights.append("✓ 初回リピート率が良好です。顧客満足度の高いサービス提供ができています。")
        elif basic['first_repeat_rate'] >= 30:
            insights.append("△ 初回リピート率は平均的です。さらなる向上の余地があります。")
        else:
            insights.append("✗ 初回リピート率が低めです。サービス品質の見直しが必要かもしれません。")
        
        # 目標達成状況
        overall_achievement = target['overall_achievement']
        if overall_achievement >= 80:
            insights.append("✓ 全体的に目標達成率が高く、優秀な成果を上げています。")
        elif overall_achievement >= 60:
            insights.append("△ 目標達成率は中程度です。特定の段階での改善が必要です。")
        else:
            insights.append("✗ 目標達成率が低いです。包括的な改善施策が必要です。")
        
        # 最も改善が必要な段階
        worst_stage = min(target['achievement_rates'].items(), key=lambda x: x[1])
        stage_names = {
            'first_repeat': '初回リピート',
            'second_repeat': '2回目リピート',
            'third_repeat': '3回目リピート'
        }
        worst_stage_name = stage_names.get(worst_stage[0], worst_stage[0])
        insights.append(f"最も改善が必要な段階: {worst_stage_name} (達成率: {worst_stage[1]:.1f}%)")
        
        # スタイリスト分析からの示唆
        stylist = results['stylist_analysis']
        if stylist['stylist_stats']:
            top_rate = stylist['stylist_stats'][0]['x_plus_rate']
            if len(stylist['stylist_stats']) > 1:
                avg_rate = sum(s['x_plus_rate'] for s in stylist['stylist_stats']) / len(stylist['stylist_stats'])
                if top_rate - avg_rate > 10:
                    insights.append(f"スタイリスト間での成果にばらつきがあります。"
                                   f"トップスタイリストのノウハウを他スタッフに共有することを推奨します。")
        
        # クーポン分析からの示唆
        coupon = results['coupon_analysis']
        if coupon['coupon_stats']:
            best_coupon = coupon['best_coupon']
            insights.append(f"「{best_coupon['name']}」が最も効果的なクーポンです。"
                           f"このタイプのクーポンの拡充を検討してください。")
        
        # 期間分析からの示唆
        period = results['period_analysis']
        if period['avg_days'] > 0:
            if period['avg_days'] <= 30:
                insights.append("初回リピートまでの期間が短く、顧客エンゲージメントが高いです。")
            elif period['avg_days'] <= 60:
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