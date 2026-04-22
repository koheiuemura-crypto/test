# -*- coding: utf-8 -*-
"""
OMNIA（データ集計・ケース課題2）
================================

利用データ（すべて「データ集計・DataStorage」配下）:
    - order_data/ … 注文マスタ（order_no をキーに他テーブルと結合）
    - delivery_status_history_data/ … 配達ステータス履歴（created_date は UNIX 秒の想定）
    - driver_shop_arrival_history_data/ … ドライバーの店舗到着時刻など

実行環境:
    - ローカル（Cursor / VS Code）: 本ファイルと同じフォルダに「データ集計・DataStorage」がある前提。
    - Google Colab: Drive マウント後、MyDrive 配下の候補パスも探索する。

課題の流れ（概要）:
    課題1〜3: 読込・重複除去・マージ
    課題4〜6: フィルタ・型整形・日付変換
    課題7〜8: 遅延率集計・工程別要因分析
    課題9: output フォルダの Excel（.xlsx）への集約出力と（Colab 時）Google スプレッドシートへの書き込み
    課題11: 課題8整形前データをスプレッドシート「Update_元データ」に抽出（シート上で関数練習用）
"""

# =============================================================================
# ライブラリ読み込み
# =============================================================================
# sys … Colab かどうか（google.colab が読み込まれているか）を判定するために使う。
# pandas … 表データの読み込み・結合・集計の中心。
# pathlib.Path … Windows / Mac 共通のパス操作。
# %%
import sys
import pandas as pd
from pathlib import Path


def print_step_banner(lesson_id: str, title: str, *description_lines: str) -> None:
    """課題の区切りをはっきり表示する（ログを追いやすくする）。"""
    bar = "=" * 70
    print(f"\n{bar}\n{lesson_id} {title}\n{bar}")
    for line in description_lines:
        print(f"  {line}")


def print_dataframe_digest(name: str, df: pd.DataFrame, *, n_head: int = 3, max_col_print: int = 14) -> None:
    """
    DataFrame の「ざっくり中身」を標準出力に出す。
    Jupyter ではセルの最後に df と書けば表示されるが、.py では print が必要。
    """
    print(f"\n── 【{name}】概要 ──")
    print(f"  行数 × 列数: {df.shape[0]:,} × {df.shape[1]}")
    col_list = [str(c) for c in df.columns]
    preview = ", ".join(col_list[:max_col_print])
    if len(col_list) > max_col_print:
        preview += f", …（ほか {len(col_list) - max_col_print} 列）"
    print(f"  列名: {preview}")
    if n_head > 0 and len(df) > 0:
        print(f"  先頭 {n_head} 行:")
        print(df.head(n_head).to_string(max_cols=max_col_print))


def resolve_data_storage() -> Path:
    """
    教材用 CSV が置いてある「データ集計・DataStorage」フォルダの絶対パスを返す。

    探索順序の意図:
        1) 本スクリプトと同じディレクトリ配下 … ローカルで .py を実行するとき最も確実。
        2) Colab の /content/drive/MyDrive 配下のよくある配置 … ノートブック教材のパス想定。
        3) カレントワーキングディレクトリ配下 … ターミナルの cwd がプロジェクトルートのとき。

    Returns:
        存在が確認できたディレクトリの Path（絶対パス）。

    Raises:
        FileNotFoundError: いずれの候補にもフォルダが無い場合。デバッグ用に候補一覧をメッセージに含める。
    """
    # 候補をリストに溜め、先に見つかったものを採用する（優先順位は上から順）。
    candidates: list[Path] = []

    # Jupyter で %run した場合など __file__ が無いことがあるためガードする。
    if "__file__" in globals():
        candidates.append(Path(__file__).resolve().parent / "データ集計・DataStorage")

    # Colab では google.colab が読み込まれる。ローカルではこの分岐に入らない。
    if "google.colab" in sys.modules:
        drive_base = Path("/content/drive/MyDrive")
        candidates.append(drive_base / "OMNIA/考える/アナリティクス/データ集計・DataStorage")
        candidates.append(drive_base / "OMNIA/アナリティクス/データ集計・DataStorage")

    # どこから実行したか不明なときのフォールバック。
    candidates.append(Path.cwd() / "データ集計・DataStorage")

    for p in candidates:
        if p.is_dir():
            return p.resolve()
    raise FileNotFoundError(
        "データ集計・DataStorage が見つかりません。確認したパス: " + ", ".join(str(p) for p in candidates)
    )


# -----------------------------------------------------------------------------
# Google Colab 専用: Google ドライブを /content/drive にマウントする。
# ローカル実行時は google.colab が import できないため、このブロック全体をスキップする。
# mount 時に認可リンクが表示されるのは正常動作。
# -----------------------------------------------------------------------------
if "google.colab" in sys.modules:
    from google.colab import drive

    drive.mount("/content/drive")

# 以降の読み込みはすべてこのルートからの相対パスで行う。
DATA_STORAGE = resolve_data_storage()
print("\n【環境】データフォルダを解決しました。")
print(f"  DATA_STORAGE = {DATA_STORAGE}")

# =============================================================================
# 課題 1: 複数の日付の order_data CSV を1つの DataFrame にまとめる
# =============================================================================
# やること: 「order_data_2022_12_*.csv」に一致するファイルを全部読み、縦に足し合わせる。
# 結果: df_orders に「12月分の全注文行」が入る（ファイルが何日分あるかで行数が変わる）。
# %%
print_step_banner(
    "課題1",
    "order_data の複数CSVを1つの DataFrame に結合",
    "パターン order_data_2022_12_*.csv に一致するファイルだけ読む（誤読込み防止）。",
    "pd.concat(..., ignore_index=True) で行インデックスを 0 から振り直す。",
)

# Path / "サブフォルダ名" … Windows でも Mac でも同じ書き方でパスをつなげる。
order_dir = DATA_STORAGE / "order_data"

# sorted(glob(...)) … ワイルドカード * に一致するファイル名を、名前順に並べたリストにする。
paths_orders = sorted(order_dir.glob("order_data_2022_12_*.csv"))
if not paths_orders:
    # ファイル見つからないとき強制終了
    raise FileNotFoundError(
        f"order_data の CSV が見つかりません: {order_dir} / order_data_2022_12_*.csv"
    )
print(f"  読み込み対象ファイル数: {len(paths_orders)} 本")
print(f"  例（先頭3本）: {[p.name for p in paths_orders[:3]]}")

# [pd.read_csv(p) for p in paths_orders] … 各 CSV を1つずつ DataFrame にし、リストにする（内包表記）。
dfs_orders = [pd.read_csv(p) for p in paths_orders]

# pd.concat(..., ignore_index=True) … 縦に連結。ignore_index で 0,1,2,… の連番にし直す。
df_orders = pd.concat(dfs_orders, ignore_index=True)

print_dataframe_digest("df_orders（課題1の結果）", df_orders, n_head=3)
# 実行結果の目安: 行数は「1日あたり約1万行 × 日数」程度。列は order_no, time_type, … など注文マスタの列。

# %% [markdown]
# # 課題 2: データの重複行を削除

# =============================================================================
# 課題 2: 完全に同一の行（全列一致）を重複として削除する
# =============================================================================
# やること: 全列の値がそっくり同じ行は2行目以降を削除する（誤って二重に入った行の除去）。
# 結果: df_dedup。通常は件数が少し減るか、ほぼ同じ。
# %%
print_step_banner(
    "課題2",
    "完全に同じ行の重複を削除",
    "duplicated() … 2行目以降の重複を True にするマスク。",
    "drop_duplicates() … 重複行を落とし、先頭行だけ残す（既定）。",
)

df_dedup = df_orders.copy()
n_before = len(df_dedup)
dup_mask = df_dedup.duplicated()
n_dup = int(dup_mask.sum())
df_dedup = df_dedup.drop_duplicates().reset_index(drop=True)
n_after = len(df_dedup)

print(f"\n【課題2の実行結果】")
print(f"  削除前: {n_before:,} 行  /  重複として数えた行: {n_dup:,} 行  /  削除後: {n_after:,} 行")
print_dataframe_digest("df_dedup（課題2の結果）", df_dedup, n_head=2)

# %% [markdown]
# ※データフレームの行数を求める

# =============================================================================
# （補足）行数の検算用に、もう一度だけ同じ glob で読み直す
# =============================================================================
# 注意: 課題1の df_orders と行数が一致すれば、読み込みは整合している。
# %%
paths_orders = sorted((DATA_STORAGE / "order_data").glob("order_data_2022_12_*.csv"))
dfs_orders = [pd.read_csv(p) for p in paths_orders]
df_orders_rowcount = pd.concat(dfs_orders, ignore_index=True)
print(f"\n【補足・検算】再読込みした行数: {len(df_orders_rowcount):,}（課題1の df_orders と同じになるはず）")

# %% [markdown]
# # 課題 3: データをマージする

# =============================================================================
# 課題 3: order_no をキーに left join で3表を横に結合する
# =============================================================================
# やること: 左＝注文マスタ、右＝配達ステータス履歴・店舗到着履歴を order_no でつなぐ。
# left join … 左の行はすべて残す。右に対応が無い注文は NaN（欠損）になる。
# 注意: order と driver 表に両方 created_at があると、2回目の結合後は created_at_x / created_at_y に分かれる。
# %%
print_step_banner(
    "課題3",
    "3つの表を order_no でマージ（left join）",
    "1回目: 注文 × delivery_status_history（クルー検索開始のUNIX時刻など）",
    "2回目: その結果 × driver_shop_arrival_history（店舗到着時刻など）",
)

orders_m = df_orders.copy()

deliv_dir = DATA_STORAGE / "delivery_status_history_data"
paths_deliv = sorted(deliv_dir.glob("delivery_status_history_data_2022_12_*.csv"))
if not paths_deliv:
    raise FileNotFoundError(f"delivery_status_history の CSV が見つかりません: {deliv_dir}")
dfs_deliv = [pd.read_csv(p) for p in paths_deliv]
df_delivery = pd.concat(dfs_deliv, ignore_index=True)
print(f"  delivery_status_history: {len(paths_deliv)} ファイル → {len(df_delivery):,} 行")

drv_dir = DATA_STORAGE / "driver_shop_arrival_history_data"
paths_drv = sorted(drv_dir.glob("driver_shop_arrival_history_data_2022_12_*.csv"))
if not paths_drv:
    raise FileNotFoundError(f"driver_shop_arrival_history の CSV が見つかりません: {drv_dir}")
dfs_drv = [pd.read_csv(p) for p in paths_drv]
df_driver = pd.concat(dfs_drv, ignore_index=True)
print(f"  driver_shop_arrival_history: {len(paths_drv)} ファイル → {len(df_driver):,} 行")

merged = orders_m.merge(df_delivery, on="order_no", how="left")
merged = merged.merge(df_driver, on="order_no", how="left")

print(f"\n【課題3の実行結果】")
print(f"  マージ後の行数: {len(merged):,}（左の注文行数と同じ。1注文に履歴が複数あると行が増える場合あり）")
print(f"  マージ後の列数: {merged.shape[1]}（suffix _x/_y が付いた列に注意）")
print_dataframe_digest("merged（課題3の結果）", merged, n_head=2)
# （参考イメージ）df_delivery は created_date（UNIX秒）と order_no など。
# （参考イメージ）マージ後は created_date, shop_arrival_at が付き、created_at は _x=注文側・_y=到着履歴側。

# =============================================================================
# 課題 4: データをフィルターして抽出する
# =============================================================================
# やること: 分析に使う注文だけ残す（教材どおりの条件）。
#   order_state == 4 … 調理完了など「完了系」の状態。
#   receive_type == 2 … デリバリー。
# & は「かつ」、条件は ( ) でくくる。
# .copy() … フィルタ後の「ビュー」ではなくコピーを作り、後で警告が出にくくする。
# %%
print_step_banner(
    "課題4",
    "条件で行を絞り込み（フィルタ）",
    "order_state == 4 かつ receive_type == 2 の行だけ残す。",
)

filtered = merged[(merged["order_state"] == 4) & (merged["receive_type"] == 2)].copy()
print(f"\n【課題4の実行結果】")
print(f"  マージ後: {len(merged):,} 行 → フィルター後: {len(filtered):,} 行")
print_dataframe_digest("filtered（課題4の結果）", filtered, n_head=2)
# =============================================================================
# 課題 5: データを整形する
# =============================================================================
# やること:
#   - delivery の created_date（UNIX秒の数値）を日時型に変える。
#   - 注文側の created_at が created_at_x になっているなら、分かりやすく created_at に戻す。
# 結果: df_shape（課題6で日付列をまとめて変換する土台）。
# %%

print_step_banner(
    "課題5",
    "UNIX秒の列を日時にし、列名を整理",
    "unix_to_datetime() … pd.to_datetime(..., unit='s') でエポック秒→日時。",
)


def unix_to_datetime(series: pd.Series) -> pd.Series:
    """
    UNIX エポックからの秒を pandas の日時型に変換する。
    Args:
        series: 数値（秒）。文字列が混じる場合は errors='coerce' で NaT に落とす。
    Returns:
        dtype が datetime64[ns] に近い Series。
    """
    return pd.to_datetime(series, unit="s", errors="coerce")

df_shape = filtered.copy()
# delivery 側の created_date は CSV 上数値（UNIX 秒）の想定。
if "created_date" in df_shape.columns:
    df_shape["created_date"] = unix_to_datetime(df_shape["created_date"])

# 1回目の merge で注文側の created_at が created_at_x にリネームされている場合、s
# 分析しやすい名前 created_at に戻す（存在するときだけ）。
if "created_at_x" in df_shape.columns:
    df_shape = df_shape.rename(columns={"created_at_x": "created_at"})


df_shape.head()
"""
order_no	time_type	order_state	receive_type	approve_date	created_at	driver_id_x	delivery_date	latest_delivery_date	accept_date	pickup_date	pass_date	local_area_flag	created_date	driver_id_y	shop_arrival_at	created_at_y
0	BY534M	1	4	2	2022/12/1 0:00	2022/12/1 0:00	5606.0	2022/12/1 0:53	2022/12/1 1:23	2022/12/1 0:07	2022/12/1 0:23	2022/12/1 0:44	0	2022-11-30 15:07:22	5606.0	2022/12/1 0:16	2022/12/1 0:16
1	GV384O	1	4	2	2022/12/1 0:12	2022/12/1 0:03	76304.0	2022/12/1 0:36	2022/12/1 1:06	2022/12/1 0:14	2022/12/1 0:26	2022/12/1 0:48	0	2022-11-30 15:13:01	76304.0	2022/12/1 0:25	2022/12/1 0:25
2	WK460E	1	4	2	2022/12/1 0:06	2022/12/1 0:05	17544.0	2022/12/1 0:48	2022/12/1 1:18	2022/12/1 0:13	2022/12/1 0:17	2022/12/1 0:32	0	2022-11-30 15:12:56	17544.0	2022/12/1 0:15	2022/12/1 0:15
"""
# =============================================================================
# 課題 6: for文を使ってループ処理をする
# =============================================================================
# やること: 日付が「文字列のまま」の列を、for で1列ずつ pd.to_datetime にかける。
# errors="coerce" … 変換できない値は NaT（欠損の日時）にする。後で dropna しやすい。
# 結果: df_loop（課題7以降の主役の DataFrame）。
# %%
print_step_banner(
    "課題6",
    "for 文で複数列をまとめて datetime 型へ",
    "date_cols に名前が載っていて、かつ実際の列が存在するものだけ変換する。",
)

date_cols = [
    "approve_date",
    "created_at",
    "delivery_date",
    "latest_delivery_date",
    "accept_date",
    "pickup_date",
    "pass_date",
    "shop_arrival_at",
    "created_at_y",  # 2回目 merge で付いたドライバ側 created_at
]

_converted: list[str] = []
df_loop = df_shape.copy()
for col in date_cols:
    if col in df_loop.columns:
        df_loop[col] = pd.to_datetime(df_loop[col], errors="coerce")
        _converted.append(col)

print(f"\n【課題6の実行結果】 datetime に変換した列: {len(_converted)} 本 → {_converted}")
_dt_subset = [c for c in date_cols if c in df_loop.columns]
print("  変換後の dtype（抜粋）:")
print(df_loop.dtypes.loc[_dt_subset].to_string())
print_dataframe_digest("df_loop（課題6の結果・日付列は datetime）", df_loop, n_head=2)

# =============================================================================
# 課題 7: 遅延している注文の割合を集計する
# =============================================================================
# やること:
#   1) order_no ごとに1行にまとめる（マージで同じ注文が複数行になっているため）。
#   2) 遅延フラグ: 実際の配達完了 pass_date が、最遅期限 latest_delivery_date を過ぎていれば True。
#      （カラム説明書: latest は配達予定＋30分。超過＝遅延）
#   3) 承認日 approve_date の「日」単位でグループ化し、遅延率 delay_rate を出す。
# 結果: df_delay_daily（日次サマリ）、df_delay_base（注文1行単位のベース表）。
# %%
print_step_banner(
    "課題7",
    "遅延フラグと承認日別の遅延率",
    "drop_duplicates(subset=['order_no'], keep='first') … 1注文1行に圧縮。",
    "is_delayed = pass_date > latest_delivery_date … 実完了が最遅期限を超えたか。",
)

df_orders_unique = df_loop.drop_duplicates(subset=["order_no"], keep="first").copy()
print(f"\n【課題7・中間】 order_no 重複除去後: {len(df_orders_unique):,} 行")

_delay_required = ["approve_date", "delivery_date", "latest_delivery_date", "pass_date"]
df_delay_base = df_orders_unique.dropna(subset=_delay_required).copy()
print(f"  遅延判定に必要な4列に欠損がない行: {len(df_delay_base):,} 行（欠損行は除外済み）")

df_delay_base["is_delayed"] = df_delay_base["pass_date"] > df_delay_base["latest_delivery_date"]
_n_delayed = int(df_delay_base["is_delayed"].sum())
# print(df_delay_base[df_delay_base["is_delayed"]].head()); print(len(df_delay_base[df_delay_base["is_delayed"]])) ;
print(f"  遅延と判定された注文（全体）: {_n_delayed:,} / {len(df_delay_base):,}")

# normalize(): 時刻を 00:00:00 にそろえた日付（同日のグルーピング用）。
df_delay_base["approve_date_day"] = df_delay_base["approve_date"].dt.normalize()

# groupby.agg: 日付ごとに件数と遅延件数を集計。
# assign で delay_rate を追加（pandas のチェインスタイル）。
df_delay_daily = (
    # 1. 承認日（時刻を 00:00:00 に切り捨てた列）でグループ化する
    # dropna=False: 承認日が欠損しているデータも無視せず集計に含める設定
    df_delay_base.groupby("approve_date_day", dropna=False)
    
    # 2. グループごとに集計（Aggregation）を行う
    # n_orders: 注文番号の数をカウント（分母）
    # n_delayed: 遅延フラグ(True=1, False=0)の合計値を計算（分子）
    .agg(
        n_orders=("order_no", "count"), 
        n_delayed=("is_delayed", "sum")
    )
    
    # 3. 新しい列「delay_rate（遅延率）」を追加する
    # lambda d: その時点のデータフレームを参照し、(分子 / 分母) を計算
    # .round(4): 小数点第4位で四捨五入（例: 0.1234 = 12.34%）
    .assign(delay_rate=lambda d: (d["n_delayed"] / d["n_orders"]).round(4))
    
    # 4. グループ化解除：インデックスになっていた「承認日」を普通の列に戻す
    .reset_index()
    
    # 5. 列名を「承認日」という日本語の名前に書き換える（出力・レポート用）
    .rename(columns={"approve_date_day": "承認日"})
)

""" 
df_delay_daily
承認日	n_orders	n_delayed	delay_rate
0	2022-12-01	10626	87	0.0082
1	2022-12-02	10705	80	0.0075
2	2022-12-03	13951	158	0.0113
3	2022-12-04	15911	205	0.0129
4	2022-12-05	22	8	0.3636
"""

len(df_delay_daily)

print("【課題7】承認日別 遅延率（delay_rate = n_delayed / n_orders）")
print(df_delay_daily.to_string(index=False))
print(f"遅延注文数（全体）: {df_delay_base['is_delayed'].sum()} / {len(df_delay_base)}")

# 課題11: スプレッドシート「Update_元データ」用。課題8で付与する工程時間（h_*）・ボトルネック列は含まない。
df_update_source_pre_task8 = df_delay_base.copy()

# %% [markdown]
# # 課題 8: 遅延の要因分析
# 遅延注文に限定し、各工程の所要時間（時間）を算出して要約する。ボトルネックは「最も時間が長かった工程」を注文ごとに特定し件数集計する。

# =============================================================================
# 課題 8: 遅延注文のみを対象に工程間の経過時間を分解する
# =============================================================================
# 工程の区切りはカラム説明書の時系列に合わせる:
#   店舗承認 → クルーが配達を受諾(accept_date) → 店舗到着(shop_arrival_at) → ピックアップ → 配達完了(pass_date)
# 注意: 「店舗到着〜受付」のように *到着を先* にすると、受諾が到着より前のデータが多く、
#       (受諾 - 到着) が負に偏る。正しくは「受諾〜店舗到着」＝ accept_date → shop_arrival_at の経過時間。
# %%


def _stage_hours(from_s: pd.Series, to_s: pd.Series) -> pd.Series:
    """
    終了時刻 - 開始時刻 を「時間」単位の float Series で返す。

    注意:
        時刻が逆転していると負の値になる（データ不整合や定義の見直しの手がかり）。
    """
    return (to_s - from_s).dt.total_seconds() / 3600.0


df_delayed = df_delay_base[df_delay_base["is_delayed"]].copy()

# 1. 工程定義を「承認」から「完了」まで完全に網羅
_stage_defs = [
    ("h_受諾待ち（承認〜受諾）", "approve_date", "accept_date"),     # 追加：マッチング時間
    ("h_クルー移動（受諾〜到着）", "accept_date", "shop_arrival_at"),
    ("h_店舗待機（到着〜ピックアップ）", "shop_arrival_at", "pickup_date"),
    ("h_最終配送（ピックアップ〜完了）", "pickup_date", "pass_date"),
]

# 2. 工程計算のループ（負の値を NaN にして統計を汚さない）
stage_col_names = []
for col_name, c_from, c_to in _stage_defs:
    if c_from in df_delayed.columns and c_to in df_delayed.columns:
        diff_hours = (df_delayed[c_to] - df_delayed[c_from]).dt.total_seconds() / 3600.0
        
        # 負の値（逆転データ）は実態を表さないため NaN に置換
        df_delayed[col_name] = diff_hours.where(diff_hours >= 0)
        stage_col_names.append(col_name)

# 3. ボトルネック判定（修正後の全工程で比較）
if stage_col_names and not df_delayed.empty:
    _stage_only = df_delayed[stage_col_names]
    # 行ごとに最大値を持つ列名を取得
    df_delayed["bottleneck_stage"] = _stage_only.idxmax(axis=1, skipna=True)
    _label_map = {c: c.replace("h_", "") for c in stage_col_names}
    df_delayed["bottleneck_stage_label"] = df_delayed["bottleneck_stage"].map(_label_map)

# 4. サマリー集計（負の値のカウントロジック含む）
_rows = []
for c in stage_col_names:
    s = df_delayed[c].dropna()
    if s.empty: continue
    
    # 元の計算で負の値だった数を正確に取得
    idx = stage_col_names.index(c)
    raw_diff = (df_delayed[_stage_defs[idx][2]] - df_delayed[_stage_defs[idx][1]]).dt.total_seconds()
    neg_count = int((raw_diff < 0).sum())

    _rows.append({
        "工程": c.replace("h_", ""),
        "平均_h": round(float(s.mean()), 3),
        "中央値_h": round(float(s.median()), 3),
        "p90_h": round(float(s.quantile(0.9)), 3),
        "負の値件数": neg_count,
        "有効件数": int(s.shape[0]),
    })

df_delay_stage_summary = pd.DataFrame(_rows)


# idxmax(axis=1): 行ごとに最大値を取る列名（工程列）を返す = その注文のボトルネック候補。
if stage_col_names and not df_delayed.empty:
    _stage_only = df_delayed[stage_col_names]
    df_delayed["bottleneck_stage"] = _stage_only.idxmax(axis=1, skipna=True)
    _label_map = {c: c.replace("h_", "").replace("_", "〜") for c in stage_col_names}
    df_delayed["bottleneck_stage_label"] = df_delayed["bottleneck_stage"].map(_label_map)
    df_delay_bottleneck = (
        df_delayed["bottleneck_stage_label"].dropna().value_counts().rename_axis("工程").reset_index(name="件数")
    )
else:
    df_delayed["bottleneck_stage"] = pd.NA
    df_delayed["bottleneck_stage_label"] = pd.NA
    df_delay_bottleneck = pd.DataFrame(columns=["工程", "件数"])

print("【課題8】遅延注文の工程別 所要時間（時間）")
print(df_delay_stage_summary.to_string(index=False))
print()
print("【課題8】ボトルネックとなった工程の件数（遅延注文）")
if not df_delay_bottleneck.empty:
    print(df_delay_bottleneck.to_string(index=False))
else:
    print("（遅延注文が0件、または工程列が算出できません）")

# スプレッドシート・Excel 明細シートに載せる列を絞る（ファイルサイズと可読性のバランス）。
# 1. 出力したい「生の日時列」と「計算した工程列」を整理
# 1. 工程定義を「一本道」で定義（属性列を含めない）
# =============================================================================
# 課題 8: 修正版（出力用データフレームの構築）
# =============================================================================

# 1. 工程定義
_stage_defs = [
    ("h_受諾待ち（承認〜受諾）", "approve_date", "accept_date"),
    ("h_クルー移動（受諾〜到着）", "accept_date", "shop_arrival_at"),
    ("h_店舗待機（到着〜ピックアップ）", "shop_arrival_at", "pickup_date"),
    ("h_最終配送（ピックアップ〜完了）", "pickup_date", "pass_date"),
]

df_delayed = df_delay_base[df_delay_base["is_delayed"]].copy()
stage_col_names = []

# 2. 工程時間の計算
for col_name, c_from, c_to in _stage_defs:
    if c_from in df_delayed.columns and c_to in df_delayed.columns:
        # 確実に float 型で計算（単位：時間）
        diff_hours = (df_delayed[c_to] - df_delayed[c_from]).dt.total_seconds() / 3600.0
        # 負の値は NaN にし、型を float に固定
        df_delayed[col_name] = diff_hours.where(diff_hours >= 0).astype(float)
        stage_col_names.append(col_name)

# 3. 整合性確認用の計算列（ここも float で計算）
df_delayed["total_lead_time_h"] = (
    (df_delayed["pass_date"] - df_delayed["approve_date"]).dt.total_seconds() / 3600.0
).astype(float)

df_delayed["delay_duration_h"] = (
    (df_delayed["pass_date"] - df_delayed["latest_delivery_date"]).dt.total_seconds() / 3600.0
).astype(float)

# 4. 明細出力用のカラム構成
_detail_cols = [
    "order_no",
    "is_delayed",
    "time_type",
    "approve_date",
    "latest_delivery_date",
    "delivery_date",
    "pass_date",            # ★追加しました
    "total_lead_time_h",
    "delay_duration_h",
    "bottleneck_stage_label"
] + stage_col_names

# 2. データの抽出と数値型の修正
df_delayed_export = df_delayed.copy()

# 数値として扱いたい列
float_cols = ["total_lead_time_h", "delay_duration_h"] + stage_col_names

for fc in float_cols:
    if fc in df_delayed_export.columns:
        # 確実に数値(float)に変換し、小数点3位で丸める
        df_delayed_export[fc] = pd.to_numeric(df_delayed_export[fc], errors='coerce').astype(float).round(3)

# 存在する列だけを最終的に抽出
_final_cols = [c for c in _detail_cols if c in df_delayed_export.columns]
df_delayed_export = df_delayed_export[_final_cols]

# %% [markdown]
# # 課題 9: スプレッドシートへのデータ抽出
# 課題7・8の集計をスプレッドシート「Update」に書き込む（gspread）。併せて `output` フォルダに Excel（.xlsx）へ保存する。
# 課題11: 課題8整形前の全件（order_no 単位・is_delayed まで）を「Update_元データ」に書き込み、シート関数で整形練習できるようにする。
# シート: https://docs.google.com/spreadsheets/d/1H9xF17WzjOqqkgkEmxnKxwOxKBPBJjrDxRMseNSYq70/edit?gid=1132557014#gid=1132557014
# 書き込み権限ないのでコピーしておきましょう

# =============================================================================
# 課題 9: ローカルに Excel 保存 +（Colab なら）スプレッドシート更新
# =============================================================================
# SPREADSHEET_ID:
#   Google スプレッドシートの URL の /d/ と /edit の間の文字列。
# WORKSHEET_TITLE:
#   タブ名（存在しなければ空のワークシートを追加する処理あり）。
# %%
# %%
# =============================================================================
# 課題 9: ローカルに Excel 保存 +（Colab なら）スプレッドシート更新
# =============================================================================
SPREADSHEET_ID = "1H9xF17WzjOqqkgkEmxnKxwOxKBPBJjrDxRMseNSYq70"
WORKSHEET_TITLE = "Update"
WORKSHEET_TITLE_DELAY_DETAIL = "Update_遅延注文詳細"
WORKSHEET_TITLE_PRE_TASK8 = "Update_元データ"

# 1つ前のディレクトリの output フォルダを起点にする
if "__file__" in globals():
    # ファイルから見て 親(アナリティクス) -> 親(プロジェクトルート) / output
    OUTPUT_DIR = Path(__file__).resolve().parent.parent / "output"
else:
    # 対話モード（Colab等）ではカレントディレクトリの1つ上
    OUTPUT_DIR = Path.cwd().parent / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EXCEL_OUTPUT_PATH = OUTPUT_DIR / "OMNIA（データ集計・ケース課題2）.xlsx"


def _df_to_sheet_matrix(title: str, df: pd.DataFrame) -> list[list]:
    """
    Google スプレッドシート API に渡す「矩形の値」のリストを作る。
    Timestamp型の値を文字列に変換し、JSONシリアライズエラーを防止する。
    """
    out: list[list] = [[title], list(df.columns.astype(str))]
    for _, row in df.iterrows():
        line = []
        for v in row.tolist():
            if pd.isna(v):
                line.append("")
            # Timestamp型や特殊なオブジェクトを文字列に変換
            elif isinstance(v, (pd.Timestamp, pd.Timedelta)) or hasattr(v, "isoformat"):
                line.append(str(v))
            else:
                line.append(v)
        out.append(line)
    return out


def _df_to_flat_matrix(df: pd.DataFrame) -> list[list]:
    """1行目が列名の素の表（課題11「Update_元データ」用）。タイトル行は付けない。"""
    out: list[list] = [list(df.columns.astype(str))]
    for _, row in df.iterrows():
        line: list = []
        for v in row.tolist():
            if pd.isna(v):
                line.append("")
            elif isinstance(v, (pd.Timestamp, pd.Timedelta)) or hasattr(v, "isoformat"):
                line.append(str(v))
            else:
                line.append(v)
        out.append(line)
    return out


def build_update_sheet_values() -> list[list]:
    """「Update」シート用に各課題の結果を縦に連結する。"""
    blocks: list[list] = []
    blocks.extend(_df_to_sheet_matrix("【課題7】承認日別 遅延率", df_delay_daily))
    blocks.append([])
    blocks.extend(_df_to_sheet_matrix("【課題8】工程別集計（遅延注文）", df_delay_stage_summary))
    blocks.append([])
    blocks.extend(_df_to_sheet_matrix("【課題8】ボトルネック件数", df_delay_bottleneck))
    blocks.append([])
    blocks.extend(_df_to_sheet_matrix("【課題8】遅延注文明細", df_delayed_export))
    return blocks


# --- Excel 出力（1ファイル・複数シート） ---
try:
    with pd.ExcelWriter(EXCEL_OUTPUT_PATH, engine="openpyxl") as writer:
        df_loop.to_excel(writer, sheet_name="課題2_整形データ", index=False)
        df_delay_daily.to_excel(writer, sheet_name="課題7_遅延率日次", index=False)
        df_delay_stage_summary.to_excel(writer, sheet_name="課題8_工程集計", index=False)
        df_delay_bottleneck.to_excel(writer, sheet_name="課題8_ボトルネック", index=False)
        df_delayed_export.to_excel(writer, sheet_name="課題8_遅延明細_修正", index=False) # ★別シート
        df_update_source_pre_task8.to_excel(writer, sheet_name="課題11_Update_元データ", index=False)
    print(f"Excel 出力完了: {EXCEL_OUTPUT_PATH.name}")
except Exception as e:
    print(f"Excel出力エラー: {e}")

# --- gspread 更新処理 ---
# --- gspread 更新処理 ---
try:
    import gspread
    from google.auth import default as google_auth_default

    # ★ 追加：新しいシート名の定義
    WORKSHEET_TITLE_DELAY_DETAIL = "Update_遅延注文詳細"

    if "google.colab" in sys.modules:
        from google.colab import auth as colab_auth

        colab_auth.authenticate_user()
        _creds, _ = google_auth_default()
        _gc = gspread.authorize(_creds)
        
        # スプレッドシートを開く
        _sh = _gc.open_by_key(SPREADSHEET_ID)
        
        # 1. 通常の Update シート（課題7, 8のサマリなど）
        try:
            _ws = _sh.worksheet(WORKSHEET_TITLE)
        except gspread.WorksheetNotFound:
            _ws = _sh.add_worksheet(title=WORKSHEET_TITLE, rows=5000, cols=30)

        _matrix = build_update_sheet_values()
        _ws.clear()
        try:
            _ws.update(values=_matrix, range_name="A1", value_input_option="USER_ENTERED")
        except TypeError:
            _ws.update("A1", _matrix, value_input_option="USER_ENTERED")
        print(f"gspread: スプレッドシート「{WORKSHEET_TITLE}」を更新しました。")

        # 2. 課題11: Update_元データ（全件）
        _matrix_raw = _df_to_flat_matrix(df_update_source_pre_task8)
        _n_raw_rows = len(_matrix_raw)
        _n_raw_cols = len(_matrix_raw[0]) if _matrix_raw else 1
        try:
            _ws_raw = _sh.worksheet(WORKSHEET_TITLE_PRE_TASK8)
        except gspread.WorksheetNotFound:
            _ws_raw = _sh.add_worksheet(
                title=WORKSHEET_TITLE_PRE_TASK8,
                rows=max(1000, _n_raw_rows + 50),
                cols=max(26, _n_raw_cols + 5),
            )
        _ws_raw.clear()
        try:
            _ws_raw.update(values=_matrix_raw, range_name="A1", value_input_option="USER_ENTERED")
        except TypeError:
            _ws_raw.update("A1", _matrix_raw, value_input_option="USER_ENTERED")
        print(f"gspread: 「{WORKSHEET_TITLE_PRE_TASK8}」を更新しました。")

        # 3. ★新規追加：Update_遅延注文詳細（pass_date と数値化された工程時間込み）
        _matrix_detail = _df_to_flat_matrix(df_delayed_export)
        _n_det_rows = len(_matrix_detail)
        _n_det_cols = len(_matrix_detail[0]) if _matrix_detail else 1
        try:
            _ws_detail = _sh.worksheet(WORKSHEET_TITLE_DELAY_DETAIL)
        except gspread.WorksheetNotFound:
            _ws_detail = _sh.add_worksheet(
                title=WORKSHEET_TITLE_DELAY_DETAIL, 
                rows=max(1000, _n_det_rows + 50), 
                cols=max(20, _n_det_cols + 5)
            )
        _ws_detail.clear()
        try:
            _ws_detail.update(values=_matrix_detail, range_name="A1", value_input_option="USER_ENTERED")
        except TypeError:
            _ws_detail.update("A1", _matrix_detail, value_input_option="USER_ENTERED")
        
        print(f"gspread: 「{WORKSHEET_TITLE_DELAY_DETAIL}」を更新しました（pass_date追加済み）。")

    else:
        print("gspread スキップ: Colab 環境ではありません。")
except Exception as e:
    print(f"gspread 更新中にエラーが発生しました: {e}")



# %% [markdown]
# ## 追加: データ集計・DataStorage を使った簡易分析
#
# 前提: 上記の `df_dedup`, `merged`, `df_loop` まで実行済み。

# =============================================================================
# 追加セル: 
# =============================================================================
# %%
od = df_dedup.copy()
od["approve_day"] = pd.to_datetime(od["approve_date"], errors="coerce").dt.date
print("【1】承認日別 注文数")
print(od.groupby("approve_day", dropna=False).size().to_string())
print()

print("【2】order_state 別件数")
print(od["order_state"].value_counts(dropna=False).sort_index().to_string())
print()
print("【3】receive_type 別件数（教材の定義に準拠）")
print(od["receive_type"].value_counts(dropna=False).sort_index().to_string())
print()

n_m = len(merged)
nz_del = merged["created_date"].notna().sum() if "created_date" in merged.columns else 0
nz_arr = merged["shop_arrival_at"].notna().sum() if "shop_arrival_at" in merged.columns else 0
print("【4】マージ後: 補助データの付与率")
print(f"注文行数: {n_m}")
if n_m:
    print(f"  delivery_status_history あり: {nz_del} ({100 * nz_del / n_m:.1f}%)")
    print(f"  driver_shop_arrival あり: {nz_arr} ({100 * nz_arr / n_m:.1f}%)")
print()

if "shop_arrival_at" in df_loop.columns and "accept_date" in df_loop.columns:
    tmp = df_loop.dropna(subset=["shop_arrival_at", "accept_date"]).copy()
    # 受諾してから店舗に着くまで（分）＝ 課題8 の h_受諾〜店舗到着 と同じ向き
    tmp["lead_min"] = (tmp["shop_arrival_at"] - tmp["accept_date"]).dt.total_seconds() / 60.0
    print("【5】受諾〜店舗到着までの分数（分）※負の値は記録順の逆転・欠損等の可能性")
    print(tmp["lead_min"].describe(percentiles=[0.5, 0.9, 0.99]).to_string())
else:
    print("【5】スキップ: shop_arrival_at / accept_date がありません")
print()

# join_orders は別期間の参考データ。無ければメッセージのみ。
jo_dir = DATA_STORAGE / "join_orders"
jo_paths = sorted(jo_dir.glob("join_orders_2021_09_*.csv"))
if jo_paths:
    jdf = pd.concat([pd.read_csv(p, sep="\t") for p in jo_paths], ignore_index=True)
    jdf["order_dt"] = pd.to_datetime(jdf["order_date"], errors="coerce")
    jdf["d"] = jdf["order_dt"].dt.date
    print("【参考】join_orders 日次件数（2021/9/25〜28 ファイル群）")
    print(jdf.groupby("d", dropna=False).size().to_string())
else:
    print("【参考】join_orders の CSV が見つかりませんでした")

