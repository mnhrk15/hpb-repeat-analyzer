"""
可視化モジュール - ダッシュボード用データ変換・チャート生成
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any
import json
import logging

logger = logging.getLogger(__name__)

class DashboardVisualizer:
    """ダッシュボード可視化クラス"""
    
    def __init__(self):
        self.chart_colors = {
            'primary': '#3b82f6',      # Blue
            'secondary': '#ef4444',    # Red
            'success': '#10b981',      # Green
            'warning': '#f59e0b',      # Yellow
            'info': '#06b6d4',         # Cyan
            'dark': '#374151',         # Gray
            'light': '#f3f4f6'         # Light Gray
        }
    
    def generate_dashboard_data(self, analysis_results: Dict) -> Dict:
        """
        ダッシュボード表示用のデータを生成
        
        Args:
            analysis_results: 分析結果辞書
            
        Returns:
            ダッシュボード用データ辞書
        """
        logger.info("ダッシュボード用データ生成開始")
        
        dashboard_data = {
            'summary_cards': self._create_summary_cards(analysis_results),
            'funnel_charts': self._create_funnel_charts(analysis_results),
            'stylist_charts': self._create_stylist_charts(analysis_results),
            'coupon_charts': self._create_coupon_charts(analysis_results),
            'target_charts': self._create_target_charts(analysis_results),
            'period_charts': self._create_period_charts(analysis_results),
            'monthly_charts': self._create_monthly_charts(analysis_results),
            'summary_tables': self._create_summary_tables(analysis_results)
        }
        
        logger.info("ダッシュボード用データ生成完了")
        return dashboard_data
    
    def _create_summary_cards(self, results: Dict) -> List[Dict]:
        """サマリーカード用データ作成"""
        basic_stats = results.get('basic_stats', {})
        if not basic_stats:
            logger.warning("_create_summary_cards: basic_stats が空または存在しません。空のカードリストを返します。")
            return []

        min_repeat_count = basic_stats.get('min_repeat_count', 'X') # デフォルト'X'

        cards = [
            {
                'title': '新規顧客総数',
                'value': basic_stats.get('total_new_customers', 0),
                'unit': '人',
                'color': 'primary',
                'icon': 'users'
            },
            {
                'title': f"{min_repeat_count}回以上リピーター数",
                'value': basic_stats.get('x_plus_repeaters', 0),
                'unit': '人',
                'color': 'success',
                'icon': 'repeat'
            },
            {
                'title': f"{min_repeat_count}回以上リピート率",
                'value': basic_stats.get('x_plus_rate', 0.0),
                'unit': '%',
                'color': 'success',
                'icon': 'trending-up'
            },
            {
                'title': '初回リピート率',
                'value': basic_stats.get('first_repeat_rate', 0.0),
                'unit': '%',
                'color': 'info',
                'icon': 'activity'
            }
        ]
        
        return cards
    
    def _create_funnel_charts(self, results: Dict) -> Dict:
        """ファネル分析チャート用データ作成"""
        funnel_data = results.get('funnel_analysis', {})
        if not funnel_data:
            logger.warning("_create_funnel_charts: funnel_analysis が空または存在しません。空のチャートデータを返します。")
            return {'stage_chart': None, 'continuation_chart': None, 'distribution_chart': None}

        stages = funnel_data.get('stages', {})
        continuation_rates = funnel_data.get('continuation_rates', {})
        repeat_distribution = funnel_data.get('repeat_distribution', {})
        cumulative_percentages = funnel_data.get('cumulative_percentages', {})

        if not stages:
            logger.warning("_create_funnel_charts: funnel_analysis.stages が空です。")
            # 少なくとも stage_chart は None になるべき
            # 他も影響を受ける可能性がある

        # ステージ別顧客数棒グラフ
        stage_chart_data_values = list(stages.values()) if stages else []
        stage_chart_labels = list(stages.keys()) if stages else []
        
        stage_chart = {
            'type': 'bar',
            'data': {
                'labels': stage_chart_labels,
                'datasets': [{
                    'label': '顧客数',
                    'data': stage_chart_data_values,
                    'backgroundColor': self.chart_colors['primary'],
                    'borderColor': self.chart_colors['primary'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'リピートステージ別顧客数'
                    },
                    'datalabels': {  
                        'display': True,
                        'align': 'end',
                        'anchor': 'end',
                        'color': '#444',
                        'font': {
                            'weight': 'bold'
                        }
                    }
                },
                'scales': {
                    'y': {
                        'beginAtZero': True
                    }
                }
            }
        }
        if not stage_chart_data_values: # データがなければNone
            stage_chart = None
            logger.warning("_create_funnel_charts: ステージ別顧客数データがないため、stage_chartはNoneになります。")


        # 継続率棒グラフ
        continuation_labels = list(continuation_rates.keys()) if continuation_rates else []
        continuation_values = list(continuation_rates.values()) if continuation_rates else []
        
        continuation_chart = {
            'type': 'bar',
            'data': {
                'labels': continuation_labels,
                'datasets': [{
                    'label': '継続率 (%)',
                    'data': continuation_values,
                    'backgroundColor': self.chart_colors['warning'],
                    'borderColor': self.chart_colors['warning'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'ステージ間継続率'
                    },
                    'datalabels': {  
                        'display': True,
                        'align': 'end',
                        'anchor': 'end',
                        'color': '#444',
                        'font': {
                            'weight': 'bold'
                        }
                    }
                },
                'scales': {
                    'y': {
                        'beginAtZero': True,
                        'max': 100
                    }
                }
            }
        }
        if not continuation_values: # データがなければNone
            continuation_chart = None
            logger.warning("_create_funnel_charts: 継続率データがないため、continuation_chartはNoneになります。")
        
        # リピート回数分布（複合グラフ）
        # repeat_distribution は {回数: 顧客数} の辞書
        # cumulative_percentages は {回数: 累積%} の辞書
        
        if not repeat_distribution:
            logger.warning("_create_funnel_charts: repeat_distribution が空です。distribution_chart は None になります。")
            distribution_chart = None
        else:
            repeat_counts = sorted(list(repeat_distribution.keys())) # 回数でソート
            repeat_values = [repeat_distribution[count] for count in repeat_counts]
            cumulative_values = [cumulative_percentages.get(count, 0) for count in repeat_counts]
            
            distribution_chart = {
                'type': 'bar',
                'data': {
                    'labels': [f"{count}回" for count in repeat_counts],
                    'datasets': [
                        {
                            'type': 'bar',
                            'label': '顧客数',
                            'data': repeat_values,
                            'backgroundColor': self.chart_colors['info'],
                            'borderColor': self.chart_colors['info'],
                            'yAxisID': 'y',
                            'datalabels': { 
                                'align': 'end',
                                'anchor': 'end'
                            }
                        },
                        {
                            'type': 'line',
                            'label': '累積割合 (%)',
                            'data': cumulative_values,
                            'borderColor': self.chart_colors['secondary'],
                            'backgroundColor': 'transparent',
                            'yAxisID': 'y1',
                            'datalabels': { 
                                'align': 'top',
                                'anchor': 'end',
                                'backgroundColor': 'rgba(255, 255, 255, 0.7)',
                                'borderRadius': 4,
                                'padding': 4
                            }
                        }
                    ]
                },
                'options': {
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': 'リピート回数分布と累積割合'
                        },
                        'datalabels': {
                            'display': True,
                            'color': '#333',
                            'font': {
                                'weight': 'bold'
                            }
                        }
                    },
                    'scales': {
                        'y': {
                            'type': 'linear',
                            'display': True,
                            'position': 'left',
                            'beginAtZero': True
                        },
                        'y1': {
                            'type': 'linear',
                            'display': True,
                            'position': 'right',
                            'beginAtZero': True,
                            'max': 100,
                            'grid': {
                                'drawOnChartArea': False,
                            }
                        }
                    }
                }
            }

        return {
            'stage_chart': stage_chart,
            'continuation_chart': continuation_chart,
            'distribution_chart': distribution_chart
        }
    
    def _create_stylist_charts(self, results: Dict) -> Dict:
        """スタイリスト分析チャート用データ作成"""
        stylist_analysis_data = results.get('stylist_analysis', {})
        basic_stats = results.get('basic_stats', {})
        min_repeat_count = basic_stats.get('min_repeat_count', 'X')
        min_stylist_customers_filter = basic_stats.get('min_stylist_customers', 0) # from analysis params

        # summary_info を先に構築。データがなくてもデフォルト値で安全に。
        summary_info = {
            'stylist_stats': stylist_analysis_data.get('stylist_stats', []),
            'min_customers_filter': stylist_analysis_data.get('min_customers_filter', min_stylist_customers_filter),
            'top_stylist': stylist_analysis_data.get('top_stylist', {'name': 'N/A', 'rate': 0.0, 'total_customers': 0}),
            'total_x_plus_repeaters': stylist_analysis_data.get('total_x_plus_repeaters', 0)
        }

        if not stylist_analysis_data or not summary_info['stylist_stats']:
            logger.warning("_create_stylist_charts: stylist_analysisデータまたはstylist_statsが空です。チャートは生成されません。")
            return {'rate_chart': None, 'summary': summary_info} # summary はデフォルト値を返す
        
        stylist_stats_list = summary_info['stylist_stats'] # 安全に取得済み

        stylists = [s.get('stylist_name', '不明') for s in stylist_stats_list]
        rates = [s.get('x_plus_rate', 0.0) for s in stylist_stats_list]
        
        if not stylists: # 念のため、リストが空の場合も考慮
            logger.warning("_create_stylist_charts: stylist_statsから有効なスタイリスト名が抽出できませんでした。")
            return {'rate_chart': None, 'summary': summary_info}

        rate_chart = {
            'type': 'bar',
            'data': {
                'labels': stylists,
                'datasets': [{
                    'label': f"{min_repeat_count}回以上リピート率 (%)",
                    'data': rates,
                    'backgroundColor': self.chart_colors.get('success', '#10b981'),
                    'borderColor': self.chart_colors.get('success', '#10b981'),
                    'borderWidth': 1
                }]
            },
            'options': {
                'indexAxis': 'y',
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': f"スタイリスト別{min_repeat_count}回以上リピート率"
                    }
                },
                'scales': {
                    'x': {
                        'beginAtZero': True,
                        'max': 100
                    }
                }
            }
        }
        
        return {
            'rate_chart': rate_chart,
            'summary': summary_info
        }
    
    def _create_coupon_charts(self, results: Dict) -> Dict:
        """クーポン分析チャート用データ作成"""
        coupon_analysis_data = results.get('coupon_analysis', {})
        basic_stats = results.get('basic_stats', {})
        min_repeat_count = basic_stats.get('min_repeat_count', 'X')
        min_coupon_customers_filter = basic_stats.get('min_coupon_customers', 0) # from analysis params

        summary_info = {
            'coupon_stats': coupon_analysis_data.get('coupon_stats', []),
            'min_customers_filter': coupon_analysis_data.get('min_customers_filter', min_coupon_customers_filter),
            'best_coupon': coupon_analysis_data.get('best_coupon', {'name': 'N/A', 'rate': 0.0, 'avg_repeat': 0.0, 'total_customers': 0})
        }

        if not coupon_analysis_data or not summary_info['coupon_stats']:
            logger.warning("_create_coupon_charts: coupon_analysisデータまたはcoupon_statsが空です。チャートは生成されません。")
            return {'rate_chart': None, 'repeat_chart': None, 'summary': summary_info}
        
        coupon_stats_list = summary_info['coupon_stats']

        coupons = [c.get('coupon_name', '不明') for c in coupon_stats_list]
        rates = [c.get('x_plus_rate', 0.0) for c in coupon_stats_list]
        avg_repeats = [c.get('avg_repeat_repeaters', 0.0) for c in coupon_stats_list]

        if not coupons: # リストが空の場合
            logger.warning("_create_coupon_charts: coupon_statsから有効なクーポン名が抽出できませんでした。")
            return {'rate_chart': None, 'repeat_chart': None, 'summary': summary_info}

        rate_chart = {
            'type': 'bar',
            'data': {
                'labels': coupons,
                'datasets': [{
                    'label': f"{min_repeat_count}回以上リピート率 (%)",
                    'data': rates,
                    'backgroundColor': self.chart_colors.get('warning', '#f59e0b'),
                    'borderColor': self.chart_colors.get('warning', '#f59e0b'),
                    'borderWidth': 1
                }]
            },
            'options': {
                'indexAxis': 'y',
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': f"クーポン別{min_repeat_count}回以上リピート率"
                    }
                },
                'scales': {
                    'x': {
                        'beginAtZero': True,
                        'max': 100
                    }
                }
            }
        }
        
        repeat_chart = {
            'type': 'bar',
            'data': {
                'labels': coupons,
                'datasets': [{
                    'label': '平均リピート回数',
                    'data': avg_repeats,
                    'backgroundColor': self.chart_colors.get('info', '#06b6d4'),
                    'borderColor': self.chart_colors.get('info', '#06b6d4'),
                    'borderWidth': 1
                }]
            },
            'options': {
                'indexAxis': 'y',
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'クーポン別平均リピート回数'
                    }
                },
                'scales': {
                    'x': {
                        'beginAtZero': True
                    }
                }
            }
        }
        
        return {
            'rate_chart': rate_chart,
            'repeat_chart': repeat_chart,
            'summary': summary_info
        }
    
    def _create_target_charts(self, results: Dict) -> Dict:
        """目標値比較チャート用データ作成"""
        target_comparison_data = results.get('target_comparison', {})
        basic_stats = results.get('basic_stats', {})

        if not target_comparison_data:
            logger.warning("_create_target_charts: target_comparisonデータがありません。")
            return {'comparison_chart': None, 'summary': {}}

        target_rates = target_comparison_data.get('target_rates', {})
        actual_rates = target_comparison_data.get('actual_rates', {})
        
        # summary は target_comparison_data 全体を渡すことが多いので、そのまま利用しつつ、
        # チャート生成に必要な主要なデータが欠けている場合はチャートを None にする
        summary_info = target_comparison_data # もし個別のデフォルト値が必要なら別途構築

        stages = ['初回リピート', '2回目リピート', '3回目リピート']
        stage_keys = ['first_repeat', 'second_repeat', 'third_repeat']
        
        target_values = [target_rates.get(key, 0.0) for key in stage_keys]
        actual_values = [actual_rates.get(key, 0.0) for key in stage_keys]

        if not target_rates or not actual_rates:
            logger.warning("_create_target_charts: target_ratesまたはactual_ratesが不足しています。チャートは生成されません。")
            return {'comparison_chart': None, 'summary': summary_info}

        comparison_chart = {
            'type': 'bar',
            'data': {
                'labels': stages,
                'datasets': [
                    {
                        'label': '実績値 (%)',
                        'data': actual_values,
                        'backgroundColor': self.chart_colors.get('primary', '#3b82f6'),
                        'borderColor': self.chart_colors.get('primary', '#3b82f6'),
                        'borderWidth': 1
                    },
                    {
                        'label': '目標値 (%)',
                        'data': target_values,
                        'backgroundColor': self.chart_colors.get('secondary', '#ef4444'),
                        'borderColor': self.chart_colors.get('secondary', '#ef4444'),
                        'borderWidth': 1
                    }
                ]
            },
            'options': {
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'リピート段階別 目標と実績'
                    }
                },
                'scales': {
                    'y': {
                        'beginAtZero': True,
                        'max': 100
                    }
                }
            }
        }
        
        return {
            'comparison_chart': comparison_chart,
            'summary': summary_info
        }
    
    def _create_period_charts(self, results: Dict) -> Dict:
        """期間分析チャート用データ作成"""
        period_analysis_data = results.get('period_analysis', {})

        if not period_analysis_data:
            logger.warning("_create_period_charts: period_analysisデータがありません。")
            return {'period_chart': None, 'summary': {}}

        period_distribution = period_analysis_data.get('period_distribution', {})
        summary_info = period_analysis_data # 必要に応じて個別にデフォルト値設定

        if not period_distribution:
            logger.warning("_create_period_charts: period_distributionが空です。チャートは生成されません。")
            return {'period_chart': None, 'summary': summary_info}
        
        periods = list(period_distribution.keys())
        # period_distribution の値は {'count': X, 'percentage': Y} のような辞書を想定
        counts = [period_distribution.get(p, {}).get('count', 0) for p in periods]

        if not periods or not any(c > 0 for c in counts): # 期間がないか、全カウントが0ならチャート不要
            logger.warning("_create_period_charts: 有効な期間データがありません。チャートは生成されません。")
            return {'period_chart': None, 'summary': summary_info}
        
        period_chart = {
            'type': 'pie',
            'data': {
                'labels': periods,
                'datasets': [{
                    'data': counts,
                    'backgroundColor': [
                        self.chart_colors.get('primary', '#3b82f6'),
                        self.chart_colors.get('success', '#10b981'),
                        self.chart_colors.get('warning', '#f59e0b'),
                        self.chart_colors.get('info', '#06b6d4'),
                        self.chart_colors.get('secondary', '#ef4444'),
                        self.chart_colors.get('dark', '#374151')
                    ]
                }]
            },
            'options': {
                'responsive': True,
                'maintainAspectRatio': False,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': '初回リピートまでの期間分布'
                    },
                    'legend': {
                        'position': 'bottom'
                    }
                }
            }
        }
        
        return {
            'period_chart': period_chart,
            'summary': summary_info
        }
    
    def _create_monthly_charts(self, results: Dict) -> Dict:
        """月別分析チャート用データ作成"""
        monthly_analysis_data = results.get('monthly_analysis', {})

        if not monthly_analysis_data:
            logger.warning("_create_monthly_charts: monthly_analysisデータがありません。")
            return {'new_customers_chart': None, 'repeat_rate_chart': None, 'summary': {}}

        monthly_new_customers = monthly_analysis_data.get('monthly_new_customers', {})
        monthly_repeat_rates_data = monthly_analysis_data.get('monthly_repeat_rates', {})
        summary_info = monthly_analysis_data # 必要に応じて個別にデフォルト値設定

        if not monthly_new_customers:
            logger.warning("_create_monthly_charts: monthly_new_customersが空です。新規顧客数チャートは生成されません。")
            new_customers_chart = None
        else:
            months_new = list(monthly_new_customers.keys())
            new_counts = list(monthly_new_customers.values())
            new_customers_chart = {
                'type': 'line',
                'data': {
                    'labels': months_new,
                    'datasets': [{
                        'label': '新規顧客数',
                        'data': new_counts,
                        'borderColor': self.chart_colors.get('primary', '#3b82f6'),
                        'backgroundColor': 'transparent',
                        'borderWidth': 2,
                        'fill': False
                    }]
                },
                'options': {
                    'responsive': True,
                    'maintainAspectRatio': False,
                    'plugins': {
                        'title': {
                            'display': True,
                            'text': '月別新規顧客数推移'
                        }
                    },
                    'scales': {
                        'y': {
                            'beginAtZero': True
                        }
                    }
                }
            }
        
        if not monthly_repeat_rates_data:
            logger.warning("_create_monthly_charts: monthly_repeat_ratesが空です。リピート率チャートは生成されません。")
            repeat_rate_chart = None
        else:
            # monthly_repeat_rates_data は {月: {'repeat_rate': X, 'new_customers': Y, 'repeaters': Z}} のような構造を想定
            # new_customers_chart と同じ月ラベルを共有するのが一般的
            months_for_rates = list(monthly_new_customers.keys()) if monthly_new_customers else list(monthly_repeat_rates_data.keys())
            months_for_rates.sort() # 月の順序を保証
            
            repeat_rates_values = [monthly_repeat_rates_data.get(m, {}).get('repeat_rate', 0.0) for m in months_for_rates]

            if not any(r > 0 for r in repeat_rates_values) and not new_customers_chart: # 両方データ無ければチャートなし
                 logger.warning("_create_monthly_charts: 有効な月次リピート率データがありません。")
                 repeat_rate_chart = None
            else:
                repeat_rate_chart = {
                    'type': 'line',
                    'data': {
                        'labels': months_for_rates, # 新規顧客数チャートのX軸と合わせる
                        'datasets': [{
                            'label': '初回リピート率 (%)',
                            'data': repeat_rates_values,
                            'borderColor': self.chart_colors.get('success', '#10b981'),
                            'backgroundColor': 'transparent',
                            'borderWidth': 2,
                            'fill': False
                        }]
                    },
                    'options': {
                        'responsive': True,
                        'maintainAspectRatio': False,
                        'plugins': {
                            'title': {
                                'display': True,
                                'text': '月別初回リピート率推移'
                            }
                        },
                        'scales': {
                            'y': {
                                'beginAtZero': True,
                                'max': 100
                            }
                        }
                    }
                }
        
        return {
            'new_customers_chart': new_customers_chart,
            'repeat_rate_chart': repeat_rate_chart,
            'summary': summary_info
        }
    
    def _create_summary_tables(self, results: Dict) -> Dict:
        """サマリーテーブル用データ作成 (例: スタイリストランキングなど)"""
        stylist_analysis = results.get('stylist_analysis', {})
        coupon_analysis = results.get('coupon_analysis', {})
        basic_stats = results.get('basic_stats', {})
        target_comparison_data = results.get('target_comparison', {})
        min_repeat_count_for_header = basic_stats.get('min_repeat_count', 'X')

        stylist_stats = stylist_analysis.get('stylist_stats', [])
        coupon_stats = coupon_analysis.get('coupon_stats', [])

        # スタイリストテーブル生成
        stylist_table_headers = ['スタイリスト名', '担当新規顧客数', '初回リピート率(%)', f"{min_repeat_count_for_header}回以上リピート率(%)"]
        stylist_table_rows = []
        if stylist_stats:
            for stat in stylist_stats:
                stylist_table_rows.append([
                    stat.get('stylist_name', '不明'),
                    stat.get('total_customers', 0),
                    round(stat.get('first_repeat_rate', 0.0), 1),
                    round(stat.get('x_plus_rate', 0.0), 1)
                ])
        else:
            logger.warning("_create_summary_tables: stylist_statsが空のため、スタイリストランキングテーブルは空になります。")
        
        stylist_table = {
            'headers': stylist_table_headers,
            'rows': stylist_table_rows
        }
        
        # クーポンテーブル生成
        coupon_table_headers = ['クーポン名', '利用顧客数', '初回リピート率(%)', f"{min_repeat_count_for_header}回以上リピート率(%)", '平均リピート回数']
        coupon_table_rows = []
        if coupon_stats:
            for stat in coupon_stats:
                coupon_table_rows.append([
                    stat.get('coupon_name', '不明'),
                    stat.get('total_customers', 0),
                    round(stat.get('first_repeat_rate', 0.0), 1),
                    round(stat.get('x_plus_rate', 0.0), 1),
                    round(stat.get('avg_repeat_repeaters', 0.0), 1)
                ])
        else:
            logger.warning("_create_summary_tables: coupon_statsが空のため、クーポンサマリーテーブルは空になります。")

        coupon_table = {
            'headers': coupon_table_headers,
            'rows': coupon_table_rows
        }

        # 目標と実績の比較テーブル生成
        target_comparison_table = []
        target_rates = target_comparison_data.get('target_rates', {})
        actual_rates = target_comparison_data.get('actual_rates', {})
        stages = [('初回リピート', 'first_repeat'), ('2回目リピート', 'second_repeat'), ('3回目リピート', 'third_repeat')]
        
        for stage_name, stage_key in stages:
            target = target_rates.get(stage_key, 0.0)
            actual = actual_rates.get(stage_key, 0.0)
            achievement = 0 if target == 0 else (actual / target) * 100
            
            target_comparison_table.append({
                'stage': stage_name,
                'target_rate': f"{target:.1f}%",
                'actual_rate': f"{actual:.1f}%",
                'achievement_rate': f"{min(achievement, 100):.1f}%"
            })
        
        # 目標達成に必要な追加顧客数テーブル生成
        additional_customers_table = []
        required_additional = target_comparison_data.get('required_additional', {})
        
        for stage_name, stage_key in stages:
            required_data = required_additional.get(stage_key, {})
            target_count = required_data.get('target_count', 0)
            current_count = required_data.get('current_count', 0)
            additional_needed = required_data.get('additional_needed', 0)
            
            additional_customers_table.append({
                'stage': stage_name,
                'target_count': target_count,
                'current_count': current_count,
                'additional_needed': additional_needed
            })

        return {
            'stylist_table': stylist_table,
            'coupon_table': coupon_table,
            'target_comparison_table': target_comparison_table,
            'additional_customers_table': additional_customers_table
        }