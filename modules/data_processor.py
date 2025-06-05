"""
データ処理モジュール - CSVファイル読み込み、データクレンジング、顧客同定
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
import chardet
import logging
from typing import List, Dict, Tuple, Optional

# ログ設定
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataProcessor:
    """データ処理クラス"""
    
    def __init__(self, default_encoding: Optional[str] = None):
        self.customer_id_columns = ['電話番号', 'フリガナ', 'お名前', '氏名(カナ)', '氏名(漢字)', 'お客様番号']
        self.required_columns = ['ステータス', '来店日', 'このサロンに行くのは初めてですか？']
        self.default_encoding = default_encoding
        
    def load_and_combine_csv_files(self, file_paths: List[str]) -> pd.DataFrame:
        """
        複数のCSVファイルを読み込み、結合・クレンジングして統一データフレームを返す
        
        Args:
            file_paths: CSVファイルパスのリスト
            
        Returns:
            結合・クレンジング済みのデータフレーム
        """
        all_dataframes = []
        
        for file_path in file_paths:
            try:
                df = self._load_single_csv(file_path)
                if df is not None and len(df) > 0:
                    all_dataframes.append(df)
                    logger.info(f"ファイル読み込み成功: {file_path} ({len(df)}件)")
                else:
                    logger.warning(f"ファイルが空またはエラー: {file_path}")
            except Exception as e:
                logger.error(f"ファイル読み込みエラー: {file_path} - {str(e)}")
                continue
        
        if not all_dataframes:
            raise ValueError("読み込み可能なCSVファイルがありません")
        
        # データフレーム結合
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"全ファイル結合完了: {len(combined_df)}件")
        
        # データクレンジング
        cleaned_df = self._clean_data(combined_df)
        logger.info(f"データクレンジング完了: {len(cleaned_df)}件")
        
        # 顧客同定・統合
        final_df = self._identify_customers(cleaned_df)
        logger.info(f"顧客同定完了: {len(final_df)}件")
        
        return final_df
    
    def _load_single_csv(self, file_path: str) -> Optional[pd.DataFrame]:
        """
        単一CSVファイルを読み込む（エンコーディング自動判定）
        
        Args:
            file_path: CSVファイルパス
            
        Returns:
            データフレーム
        """
        # エンコーディング検出
        with open(file_path, 'rb') as f:
            raw_data = f.read()
            encoding_result = chardet.detect(raw_data)
            detected_encoding = encoding_result['encoding']
        
        # 複数エンコーディングでトライ
        encodings_to_try = []
        if self.default_encoding:
            encodings_to_try.append(self.default_encoding)
        
        # 重複を避けるためにセットを使ってからリストに戻す
        potential_encodings = [
            'utf-8-sig',
            detected_encoding,
            'cp932',
            'shift_jis',
            'utf-8'
        ]
        for enc in potential_encodings:
            if enc and enc not in encodings_to_try:
                encodings_to_try.append(enc)
        
        for encoding in encodings_to_try:
            if encoding is None:
                continue
            try:
                df = pd.read_csv(file_path, encoding=encoding)
                logger.info(f"エンコーディング成功: {file_path} ({encoding})")
                return df
            except (UnicodeDecodeError, UnicodeError):
                continue
            except Exception as e:
                logger.warning(f"CSV読み込みエラー ({encoding}): {str(e)}")
                continue
        
        logger.error(f"全エンコーディングで読み込み失敗: {file_path}")
        return None
    
    def _clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        データクレンジング処理
        
        Args:
            df: 元データフレーム
            
        Returns:
            クレンジング済みデータフレーム
        """
        df = df.copy()
        original_count = len(df)
        
        # 必須カラムの存在確認
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(f"必須カラムが不足: {missing_columns}")
        
        # 来店日の処理
        df = self._clean_visit_date(df)
        
        # ステータスフィルタ
        df = df[df['ステータス'] == '済み'].copy()
        logger.info(f"ステータス='済み'でフィルタ: {len(df)}/{original_count}件")
        
        # 顧客情報のクレンジング
        df = self._clean_customer_info(df)
        
        # 電話番号の統一
        df = self._clean_phone_numbers(df)
        
        # 氏名の正規化
        df = self._clean_names(df)
        
        return df
    
    def _clean_visit_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """来店日の処理"""
        df = df.copy()
        
        if '来店日' not in df.columns:
            logger.error("来店日カラムが存在しません")
            return df
        
        # 来店日をdatetimeに変換
        def parse_date(date_str):
            if pd.isna(date_str):
                return None
            try:
                # YYYYMMDDフォーマットを想定
                date_str = str(date_str).strip()
                if len(date_str) == 8 and date_str.isdigit():
                    return datetime.strptime(date_str, '%Y%m%d')
                else:
                    # その他の日付フォーマットも試行
                    return pd.to_datetime(date_str)
            except:
                return None
        
        df['来店日_parsed'] = df['来店日'].apply(parse_date)
        
        # 有効な来店日のレコードのみ残す
        valid_dates = df['来店日_parsed'].notna()
        df = df[valid_dates].copy()
        logger.info(f"有効な来店日でフィルタ: {len(df)}件")
        
        return df
    
    def _clean_customer_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """顧客情報のクレンジング"""
        df = df.copy()
        
        # お客様番号の処理（指数表記の修正）
        if 'お客様番号' in df.columns:
            def clean_customer_number(val):
                if pd.isna(val):
                    return None
                try:
                    # 指数表記を文字列に変換
                    if isinstance(val, float):
                        return f"{val:.0f}"
                    return str(val).strip()
                except:
                    return str(val)
            
            df['お客様番号_cleaned'] = df['お客様番号'].apply(clean_customer_number)
        
        return df
    
    def _clean_phone_numbers(self, df: pd.DataFrame) -> pd.DataFrame:
        """電話番号の統一処理"""
        df = df.copy()
        
        phone_columns = [col for col in df.columns if '電話番号' in col]
        
        def normalize_phone(phone):
            if pd.isna(phone):
                return None
            # 非数値文字を除去
            phone_clean = re.sub(r'[^\d]', '', str(phone))
            return phone_clean if phone_clean else None
        
        for col in phone_columns:
            df[f'{col}_normalized'] = df[col].apply(normalize_phone)
        
        # 複数の電話番号カラムがある場合の整合性チェック
        if len(phone_columns) > 1:
            def check_phone_consistency(row):
                phones = [row[f'{col}_normalized'] for col in phone_columns]
                phones = [p for p in phones if p is not None]
                return phones[0] if phones else None
            
            df['統一電話番号'] = df.apply(check_phone_consistency, axis=1)
        elif phone_columns:
            df['統一電話番号'] = df[f'{phone_columns[0]}_normalized']
        
        return df
    
    def _clean_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """氏名の正規化処理"""
        df = df.copy()
        
        name_columns = ['フリガナ', 'お名前', '氏名(カナ)', '氏名(漢字)']
        
        def normalize_name(name):
            if pd.isna(name):
                return None
            # 全角/半角スペース除去、カタカナ統一
            name_clean = str(name).strip()
            name_clean = re.sub(r'[\s　]+', '', name_clean)  # スペース除去
            # カタカナを全角に統一（簡易版）
            name_clean = name_clean.replace('ｱ', 'ア').replace('ｶ', 'カ')  # など
            return name_clean if name_clean else None
        
        for col in name_columns:
            if col in df.columns:
                df[f'{col}_normalized'] = df[col].apply(normalize_name)
        
        # 統一氏名キーの生成
        def create_name_key(row):
            # 氏名情報を優先順位で結合
            kana = row.get('フリガナ_normalized') or row.get('氏名(カナ)_normalized')
            kanji = row.get('お名前_normalized') or row.get('氏名(漢字)_normalized')
            
            if kana and kanji:
                return f"{kana}#{kanji}"
            elif kana:
                return kana
            elif kanji:
                return kanji
            else:
                return None
        
        df['統一氏名キー'] = df.apply(create_name_key, axis=1)
        
        return df
    
    def _identify_customers(self, df: pd.DataFrame) -> pd.DataFrame:
        """顧客同定・統合処理"""
        df = df.copy()
        
        # 顧客IDの生成
        def generate_customer_id(row):
            # 優先順位: 電話番号 > 統一氏名キー > お客様番号
            phone = row.get('統一電話番号')
            name = row.get('統一氏名キー')
            customer_num = row.get('お客様番号_cleaned')
            
            if phone:
                return f"PHONE_{phone}"
            elif name:
                return f"NAME_{name}"
            elif customer_num:
                return f"CUST_{customer_num}"
            else:
                # 一意なIDを生成（行番号ベース）
                return f"UNKNOWN_{row.name}"
        
        df['顧客ID'] = df.apply(generate_customer_id, axis=1)
        
        # 顧客IDごとの重複チェック・整合性確認
        customer_groups = df.groupby('顧客ID')
        
        inconsistencies = []
        for customer_id, group in customer_groups:
            if len(group) > 1:
                # 同一顧客IDでデータ整合性をチェック
                phones = group['統一電話番号'].dropna().unique()
                names = group['統一氏名キー'].dropna().unique()
                
                if len(phones) > 1 or len(names) > 1:
                    inconsistencies.append(customer_id)
        
        if inconsistencies:
            logger.warning(f"データ整合性に問題のある顧客ID: {len(inconsistencies)}件")
        
        return df
    
    def get_new_customers(self, df: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
        """
        指定期間の新規顧客を抽出
        
        Args:
            df: 全データフレーム
            start_date: 開始日 (YYYY-MM-DD)
            end_date: 終了日 (YYYY-MM-DD)
            
        Returns:
            新規顧客データフレーム
        """
        # 期間フィルタ
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        period_filter = (df['来店日_parsed'] >= start_dt) & (df['来店日_parsed'] <= end_dt)
        period_data = df[period_filter].copy()
        
        # 新規顧客フィルタ
        if 'このサロンに行くのは初めてですか？' in df.columns:
            new_customer_filter = period_data['このサロンに行くのは初めてですか？'] == 'はい、初めてです'
            new_customers = period_data[new_customer_filter].copy()
        else:
            logger.warning("新規顧客判定カラムが存在しません")
            new_customers = period_data.copy()
        
        # 顧客ごとに最初の来店日を特定
        new_customers_first_visit = new_customers.groupby('顧客ID')['来店日_parsed'].min().reset_index()
        new_customers_first_visit.columns = ['顧客ID', '初回来店日']
        
        # 詳細情報をマージ
        new_customers_detailed = new_customers.merge(new_customers_first_visit, on='顧客ID')
        
        # 初回来店のレコードのみ抽出
        new_customers_detailed = new_customers_detailed[
            new_customers_detailed['来店日_parsed'] == new_customers_detailed['初回来店日']
        ].copy()
        
        logger.info(f"新規顧客抽出完了: {len(new_customers_detailed)}人")
        
        return new_customers_detailed 