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
        basic_stats = results['basic_stats']
        
        cards = [
            {
                'title': '新規顧客総数',
                'value': basic_stats['total_new_customers'],
                'unit': '人',
                'color': 'primary',
                'icon': 'users'
            },
            {
                'title': f"{basic_stats['min_repeat_count']}回以上リピーター数",
                'value': basic_stats['x_plus_repeaters'],
                'unit': '人',
                'color': 'success',
                'icon': 'repeat'
            },
            {
                'title': f"{basic_stats['min_repeat_count']}回以上リピート率",
                'value': basic_stats['x_plus_rate'],
                'unit': '%',
                'color': 'success',
                'icon': 'trending-up'
            },
            {
                'title': '初回リピート率',
                'value': basic_stats['first_repeat_rate'],
                'unit': '%',
                'color': 'info',
                'icon': 'activity'
            }
        ]
        
        return cards
    
    def _create_funnel_charts(self, results: Dict) -> Dict:
        """ファネル分析チャート用データ作成"""
        funnel_data = results['funnel_analysis']
        
        # ステージ別顧客数棒グラフ
        stage_chart = {
            'type': 'bar',
            'data': {
                'labels': list(funnel_data['stages'].keys()),
                'datasets': [{
                    'label': '顧客数',
                    'data': list(funnel_data['stages'].values()),
                    'backgroundColor': self.chart_colors['primary'],
                    'borderColor': self.chart_colors['primary'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'リピートステージ別顧客数'
                    }
                },
                'scales': {
                    'y': {
                        'beginAtZero': True
                    }
                }
            }
        }
        
        # 継続率棒グラフ
        continuation_labels = list(funnel_data['continuation_rates'].keys())
        continuation_values = list(funnel_data['continuation_rates'].values())
        
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
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'ステージ間継続率'
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
        
        # リピート回数分布（複合グラフ）
        repeat_counts = list(funnel_data['repeat_distribution'].keys())
        repeat_values = list(funnel_data['repeat_distribution'].values())
        cumulative_values = [funnel_data['cumulative_percentages'].get(count, 0) for count in repeat_counts]
        
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
                        'yAxisID': 'y'
                    },
                    {
                        'type': 'line',
                        'label': '累積割合 (%)',
                        'data': cumulative_values,
                        'borderColor': self.chart_colors['secondary'],
                        'backgroundColor': 'transparent',
                        'yAxisID': 'y1'
                    }
                ]
            },
            'options': {
                'responsive': True,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': 'リピート回数分布と累積割合'
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
        stylist_data = results['stylist_analysis']
        
        if not stylist_data['stylist_stats']:
            return {'rate_chart': None, 'summary': stylist_data}
        
        # X回以上リピート率横棒グラフ
        stylists = [s['stylist_name'] for s in stylist_data['stylist_stats']]
        rates = [s['x_plus_rate'] for s in stylist_data['stylist_stats']]
        
        rate_chart = {
            'type': 'horizontalBar',
            'data': {
                'labels': stylists,
                'datasets': [{
                    'label': f"{results['basic_stats']['min_repeat_count']}回以上リピート率 (%)",
                    'data': rates,
                    'backgroundColor': self.chart_colors['success'],
                    'borderColor': self.chart_colors['success'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': f"スタイリスト別{results['basic_stats']['min_repeat_count']}回以上リピート率"
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
            'summary': stylist_data
        }
    
    def _create_coupon_charts(self, results: Dict) -> Dict:
        """クーポン分析チャート用データ作成"""
        coupon_data = results['coupon_analysis']
        
        if not coupon_data['coupon_stats']:
            return {'rate_chart': None, 'repeat_chart': None, 'summary': coupon_data}
        
        # X回以上リピート率横棒グラフ
        coupons = [c['coupon_name'] for c in coupon_data['coupon_stats']]
        rates = [c['x_plus_rate'] for c in coupon_data['coupon_stats']]
        
        rate_chart = {
            'type': 'horizontalBar',
            'data': {
                'labels': coupons,
                'datasets': [{
                    'label': f"{results['basic_stats']['min_repeat_count']}回以上リピート率 (%)",
                    'data': rates,
                    'backgroundColor': self.chart_colors['warning'],
                    'borderColor': self.chart_colors['warning'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
                'plugins': {
                    'title': {
                        'display': True,
                        'text': f"クーポン別{results['basic_stats']['min_repeat_count']}回以上リピート率"
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
        
        # 平均リピート回数横棒グラフ
        avg_repeats = [c['avg_repeat_repeaters'] for c in coupon_data['coupon_stats']]
        
        repeat_chart = {
            'type': 'horizontalBar',
            'data': {
                'labels': coupons,
                'datasets': [{
                    'label': '平均リピート回数',
                    'data': avg_repeats,
                    'backgroundColor': self.chart_colors['info'],
                    'borderColor': self.chart_colors['info'],
                    'borderWidth': 1
                }]
            },
            'options': {
                'responsive': True,
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
            'summary': coupon_data
        }
    
    def _create_target_charts(self, results: Dict) -> Dict:
        """目標値比較チャート用データ作成"""
        target_data = results['target_comparison']
        
        # 目標と実績比較棒グラフ
        stages = ['初回リピート', '2回目リピート', '3回目リピート']
        target_values = [
            target_data['target_rates']['first_repeat'],
            target_data['target_rates']['second_repeat'],
            target_data['target_rates']['third_repeat']
        ]
        actual_values = [
            target_data['actual_rates']['first_repeat'],
            target_data['actual_rates']['second_repeat'],
            target_data['actual_rates']['third_repeat']
        ]
        
        comparison_chart = {
            'type': 'bar',
            'data': {
                'labels': stages,
                'datasets': [
                    {
                        'label': '実績値 (%)',
                        'data': actual_values,
                        'backgroundColor': self.chart_colors['primary'],
                        'borderColor': self.chart_colors['primary'],
                        'borderWidth': 1
                    },
                    {
                        'label': '目標値 (%)',
                        'data': target_values,
                        'backgroundColor': self.chart_colors['secondary'],
                        'borderColor': self.chart_colors['secondary'],
                        'borderWidth': 1
                    }
                ]
            },
            'options': {
                'responsive': True,
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
            'summary': target_data
        }
    
    def _create_period_charts(self, results: Dict) -> Dict:
        """期間分析チャート用データ作成"""
        period_data = results['period_analysis']
        
        if not period_data['period_distribution']:
            return {'period_chart': None, 'summary': period_data}
        
        # 期間分布円グラフ
        periods = list(period_data['period_distribution'].keys())
        counts = [period_data['period_distribution'][p]['count'] for p in periods]
        
        period_chart = {
            'type': 'pie',
            'data': {
                'labels': periods,
                'datasets': [{
                    'data': counts,
                    'backgroundColor': [
                        self.chart_colors['primary'],
                        self.chart_colors['success'],
                        self.chart_colors['warning'],
                        self.chart_colors['info'],
                        self.chart_colors['secondary'],
                        self.chart_colors['dark']
                    ]
                }]
            },
            'options': {
                'responsive': True,
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
            'summary': period_data
        }
    
    def _create_monthly_charts(self, results: Dict) -> Dict:
        """月別分析チャート用データ作成"""
        monthly_data = results['monthly_analysis']
        
        # 月別新規顧客数
        months = list(monthly_data['monthly_new_customers'].keys())
        new_counts = list(monthly_data['monthly_new_customers'].values())
        
        new_customers_chart = {
            'type': 'line',
            'data': {
                'labels': months,
                'datasets': [{
                    'label': '新規顧客数',
                    'data': new_counts,
                    'borderColor': self.chart_colors['primary'],
                    'backgroundColor': 'transparent',
                    'borderWidth': 2,
                    'fill': False
                }]
            },
            'options': {
                'responsive': True,
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
        
        # 月別初回リピート率
        repeat_rates = [monthly_data['monthly_repeat_rates'][m]['repeat_rate'] for m in months if m in monthly_data['monthly_repeat_rates']]
        
        repeat_rate_chart = {
            'type': 'line',
            'data': {
                'labels': months,
                'datasets': [{
                    'label': '初回リピート率 (%)',
                    'data': repeat_rates,
                    'borderColor': self.chart_colors['success'],
                    'backgroundColor': 'transparent',
                    'borderWidth': 2,
                    'fill': False
                }]
            },
            'options': {
                'responsive': True,
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
            'summary': monthly_data
        }
    
    def _create_summary_tables(self, results: Dict) -> Dict:
        """サマリーテーブル用データ作成"""
        
        # 目標値比較テーブル
        target_data = results['target_comparison']
        target_table = []
        
        stages = {
            'first_repeat': '初回リピート',
            'second_repeat': '2回目リピート', 
            'third_repeat': '3回目リピート'
        }
        
        for stage_key, stage_name in stages.items():
            target_table.append({
                'stage': stage_name,
                'target_rate': f"{target_data['target_rates'][stage_key]}%",
                'actual_rate': f"{target_data['actual_rates'][stage_key]}%",
                'achievement_rate': f"{target_data['achievement_rates'][stage_key]}%"
            })
        
        # 追加必要顧客数テーブル
        additional_table = []
        for stage_key, stage_name in stages.items():
            additional_data = target_data['required_additional'][stage_key]
            additional_table.append({
                'stage': stage_name,
                'target_count': additional_data['target_count'],
                'current_count': additional_data['current_count'],
                'additional_needed': additional_data['additional_needed']
            })
        
        return {
            'target_comparison_table': target_table,
            'additional_customers_table': additional_table
        }
    
    def get_chart_data(self, chart_type: str, analysis_results: Dict) -> Dict:
        """
        特定のチャートデータを取得
        
        Args:
            chart_type: チャート種別
            analysis_results: 分析結果
            
        Returns:
            チャートデータ
        """
        dashboard_data = self.generate_dashboard_data(analysis_results)
        
        chart_map = {
            'funnel_stages': dashboard_data['funnel_charts']['stage_chart'],
            'funnel_continuation': dashboard_data['funnel_charts']['continuation_chart'],
            'funnel_distribution': dashboard_data['funnel_charts']['distribution_chart'],
            'stylist_rates': dashboard_data['stylist_charts']['rate_chart'],
            'coupon_rates': dashboard_data['coupon_charts']['rate_chart'],
            'coupon_repeats': dashboard_data['coupon_charts']['repeat_chart'],
            'target_comparison': dashboard_data['target_charts']['comparison_chart'],
            'period_distribution': dashboard_data['period_charts']['period_chart'],
            'monthly_new': dashboard_data['monthly_charts']['new_customers_chart'],
            'monthly_repeat': dashboard_data['monthly_charts']['repeat_rate_chart']
        }
        
        return chart_map.get(chart_type, {'error': 'Chart type not found'}) 