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

        # RepeatAnalyzerが必要とする可能性のある主要カラムの存在確認ログ
        expected_columns_for_analysis = [
            '来店日', '顧客ID', 'スタイリスト名', 
            '予約時HotPepperBeautyクーポン', '予約時合計金額', '性別', '予約時メニュー',
            'このサロンに行くのは初めてですか？' # RepeatAnalyzerでも参照
        ]
        logger.info("最終DataFrameのカラム構成チェック:")
        for col in expected_columns_for_analysis:
            if col in final_df.columns:
                logger.info(f"  ✅ カラム '{col}' は存在します。型: {final_df[col].dtype}")
            else:
                logger.warning(f"  ⚠️ カラム '{col}' は存在しません。")
        
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
            # 必須カラムがない場合は警告を出し、処理を継続するが、カラムがないことによるエラーは下流で発生する可能性あり
            logger.warning(f"必須カラムが不足: {missing_columns}")
        
        # 来店日の処理 (エラーの場合、該当行がフィルタされる可能性)
        df = self._clean_visit_date(df) # '来店日' カラムがdatetime型になる
        
        # ステータスフィルタ (ステータスカラムが存在する場合のみ)
        # if 'ステータス' in df.columns:
        #     df = df[df['ステータス'] == '済み'].copy()
        #     logger.info(f"ステータス='済み'でフィルタ: {len(df)}/{original_count}件 (フィルタ前件数は来店日処理後)")
        # else:
        #     logger.warning("'ステータス' カラムが存在しないため、フィルタリングをスキップします。")

        # 真偽値フラグのクリーニング (「このサロンに行くのは初めてですか？」など)
        df = self._clean_boolean_flags(df)

        # 顧客情報のクレンジング
        df = self._clean_customer_info(df)
        
        # 電話番号の統一
        df = self._clean_phone_numbers(df)
        
        # 氏名の正規化
        df = self._clean_names(df)
        
        # スタイリスト名の正規化（スペース除去）
        if 'スタイリスト名' in df.columns:
            logger.info("スタイリスト名の正規化（スペース除去）を実行します。")
            df['スタイリスト名'] = df['スタイリスト名'].apply(
                lambda x: re.sub(r'[\s　]+', '', str(x)) if pd.notna(x) else x
            )
        
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
        
        # '来店日_parsed' カラムが存在する場合、元の '来店日' カラムを削除し、'来店日_parsed' を '来店日' にリネーム
        if '来店日_parsed' in df.columns:
            if '来店日' in df.columns:
                df.drop(columns=['来店日'], inplace=True)
            df.rename(columns={'来店日_parsed': '来店日'}, inplace=True)
            
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
            # *_normalized カラムが存在すればそれを使用し、なければ元のカラムを試す
            kana_col_candidates = ['フリガナ_normalized', '氏名(カナ)_normalized', 'フリガナ', '氏名(カナ)']
            kanji_col_candidates = ['お名前_normalized', '氏名(漢字)_normalized', 'お名前', '氏名(漢字)']
            
            kana = next((row.get(col) for col in kana_col_candidates if row.get(col) and pd.notna(row.get(col))), None)
            kanji = next((row.get(col) for col in kanji_col_candidates if row.get(col) and pd.notna(row.get(col))), None)
            
            if kana and kanji:
                return f"{str(kana).strip()}#{str(kanji).strip()}" # strip() を追加して前後の空白を除去
            elif kana:
                return str(kana).strip()
            elif kanji:
                return str(kanji).strip()
            else:
                return None 

        df['統一氏名キー'] = df.apply(create_name_key, axis=1)
        
        # ここでは正規化とキー生成のみ。顧客同定は _identify_customers で行う。
        return df
    
    def _clean_boolean_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """特定のフラグ列をブール型に変換する"""
        df = df.copy()
        column_name = 'このサロンに行くのは初めてですか？'
        if column_name not in df.columns:
            logger.warning(f"カラム '{column_name}' が存在しないため、ブール変換をスキップします。")
            return df

        # マッピング辞書: 文字列 -> ブール値
        # 小文字に変換して比較することで、大文字・小文字の揺れに対応
        true_values = ['true', 'yes', 'はい', 'はい、初めてです', '1']
        false_values = ['false', 'no', 'いいえ', '0']

        def map_to_bool(value):
            if pd.isna(value):
                return None # または False をデフォルトにするか検討
            str_value = str(value).lower().strip()
            if str_value in true_values:
                return True
            if str_value in false_values:
                return False
            # どちらにも該当しない場合は None (不明) または False とする
            # RepeatAnalyzerのロジックでは True 以外はリピーターとして扱われるため、NoneでもFalseでも同様の挙動になることが多い
            # ここでは None として、欠損として扱う
            logger.debug(f"カラム '{column_name}' の値 '{value}' はTrue/Falseに変換できませんでした。Noneとして扱います。")
            return None 

        df[column_name] = df[column_name].apply(map_to_bool)
        # dtypeを明示的にboolにしたいが、Noneが含まれるとobjectになるため、ここでは変換しない。
        # Pandas >= 1.0 であれば df[column_name] = df[column_name].astype("boolean") でNullable Booleanが使える。
        # 現在のPandasのバージョンと挙動に依存する。Noneを許容するならobjectのままで良い。
        # RepeatAnalyzer側では if new_customers['このサロンに行くのは初めてですか？'] == True: と比較しているので、
        # None や False は条件に合致しないため、実質的に False と同様に扱われる。

        logger.info(f"カラム '{column_name}' のブール変換処理完了。")
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
        指定された期間の新規顧客を抽出する

        Args:
            df: 全来店データ（顧客ID、来店日、このサロンに行くのは初めてですか？ を含む）
            start_date: 新規顧客抽出開始日 (YYYY-MM-DD)
            end_date: 新規顧客抽出終了日 (YYYY-MM-DD)

        Returns:
            新規顧客のデータフレーム（初回来店情報を含む）
        """
        logger.info(f"新規顧客抽出開始: 期間 {start_date} - {end_date}")

        # 日付型に変換
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
        except ValueError as e:
            logger.error(f"日付文字列の変換に失敗しました: {start_date}, {end_date} - {e}")
            raise ValueError(f"日付文字列の形式が不正です。YYYY-MM-DD形式で指定してください: {e}")


        # 必要なカラムの存在確認
        required_cols = ['顧客ID', '来店日', 'このサロンに行くのは初めてですか？'] # この後 first_visit_flag_col で参照
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"get_new_customers: 必要なカラム '{col}' がDataFrameに存在しません。")
                raise ValueError(f"必要なカラム '{col}' がDataFrameに存在しません。")

        if not pd.api.types.is_datetime64_any_dtype(df['来店日']):
            # 厳密には、ここでエラーを raise するか、変換を試みるかは設計次第
            # _clean_visit_date で変換済みのはずなので、基本的にはここに来ない想定
            logger.warning("get_new_customers: '来店日' カラムがdatetime型ではありません。事前に処理されているべきです。")
            # df['来店日'] = pd.to_datetime(df['来店日'], errors='coerce') # 強制変換する場合
            # df.dropna(subset=['来店日'], inplace=True) # 不正な日付を除去

        first_visit_flag_col = 'このサロンに行くのは初めてですか？_bool'
        if first_visit_flag_col not in df.columns:
            # _bool カラムがない場合は元のカラム名で試すが、型や値のバリエーションに注意が必要
            logger.warning(f"カラム '{first_visit_flag_col}' が見つかりません。元のカラム 'このサロンに行くのは初めてですか？' を使用しますが、ブール型への事前変換を推奨します。")
            first_visit_flag_col = 'このサロンに行くのは初めてですか？'
            if first_visit_flag_col not in df.columns: # 元のカラム名でも見つからない場合
                 logger.error(f"get_new_customers: 新規判定フラグカラム ('{first_visit_flag_col}' または 'このサロンに行くのは初めてですか？_bool') がDataFrameに存在しません。")
                 raise ValueError(f"新規判定フラグカラムが存在しません。")
            # 元カラムの場合、Trueと評価されるべき値 (e.g., "はい", 1) も考慮が必要だが、
            # ここでは _clean_boolean_flags で True/False になっていることを期待する
            # df[first_visit_flag_col] = df[first_visit_flag_col].apply(lambda x: x == True or str(x).lower() == 'はい') # より堅牢な処理


        # 新規顧客判定ロジックの変更
        df_copy = df.copy()

        if '顧客ID' not in df_copy.columns or '来店日' not in df_copy.columns:
            logger.error("get_new_customers: '顧客ID'または'来店日'カラムがdf_copyに不足しています。")
            return pd.DataFrame()

        # 1. 各顧客の全期間における最初の来店日を計算
        df_copy['全期間初回来店日'] = df_copy.groupby('顧客ID')['来店日'].transform('min')

        # 2. 条件を定義
        # 条件A: この来店が全期間初回来店である
        condition_A = (df_copy['来店日'] == df_copy['全期間初回来店日'])
        # 条件B: この来店で「初めてフラグ」がTrueまたは空白（None/NaN）である
        condition_B = (df_copy[first_visit_flag_col] == True) | (df_copy[first_visit_flag_col].isna())
        # 条件C: この来店が指定期間内である
        condition_C_period = (df_copy['来店日'] >= start_dt) & (df_copy['来店日'] <= end_dt)

        # 3. 全ての条件を満たす来店記録を抽出
        true_new_customer_visits = df_copy[condition_A & condition_B & condition_C_period]

        if true_new_customer_visits.empty:
            logger.info(f"指定期間 ({start_date} - {end_date}) に「初めてフラグTrueまたは空白」かつ「全期間で初回来店」の顧客は見つかりませんでした。")
            final_new_customers = pd.DataFrame()
        else:
            # 条件を満たす来店は顧客ごとにユニークのはず (初回来店かつフラグTrueはその顧客にとって1回のみ)
            # もし万が一、同一顧客で複数の日付がこの条件を満たす場合（データ不整合）、
            # idxmin()で最初の日付のものを取ることで一意にする。
            final_new_customers = true_new_customer_visits.loc[
                true_new_customer_visits.groupby('顧客ID')['来店日'].idxmin()
            ].copy() # .copy() をつけてSettingWithCopyWarningを回避

        # 分析結果に不要な作業列を削除
        if '全期間初回来店日' in final_new_customers.columns:
            final_new_customers.drop(columns=['全期間初回来店日'], inplace=True)
        
        if not final_new_customers.empty:
            logger.info(f"新規顧客抽出完了: {len(final_new_customers)}件。期間: {start_date} - {end_date}")
        # '来店日' カラムはこの時点でその顧客の「初回来店日」を指している
        return final_new_customers

    def get_date_range(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        """
        データフレーム内の '来店日' カラムから最初と最後の日付を取得する

        Args:
            df: データフレーム (来店日カラムはdatetime型であること)

        Returns:
            (最初の日付文字列, 最後の日付文字列) or (None, None)
        """
        if '来店日' not in df.columns or df['来店日'].isnull().all() or not pd.api.types.is_datetime64_any_dtype(df['来店日']):
            logger.warning("get_date_range: '来店日' カラムが存在しない、すべて欠損している、またはdatetime型ではありません。")
            return None, None
        
        try:
            min_date = df['来店日'].min().strftime('%Y-%m-%d')
            max_date = df['来店日'].max().strftime('%Y-%m-%d')
            return min_date, max_date
        except Exception as e:
            logger.error(f"日付範囲取得エラー: {e}")
            return None, None 