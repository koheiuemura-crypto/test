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
"""

# =============================================================================
# セル: ライブラリとデータルート解決
# =============================================================================
# sys:
#   実行中に「今 Colab かどうか」を判定するために sys.modules を参照する。
# pandas (pd):
#   表形式データの読み込み・結合・集計・出力の中心ライブラリ。
# pathlib.Path:
#   OS に依存しないパス操作（/ で連結、存在確認など）。
# %%
import sys
import os
import pandas as pd
from pathlib import Path


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
print("DATA_STORAGE =", DATA_STORAGE)

# =============================================================================
# 課題 1: 複数の日付の order_data CSV を1つの DataFrame にまとめる
# =============================================================================
# order_data ディレクトリ内の、ファイル名パターンに一致する CSV だけを対象にする。
# 教材では 2022年12月の複数日分（例: order_data_2022_12_01.csv）を想定。
# %%
# Path / "子" は pathlib の書き方で、OS 非依存でパスをつなぐ（Windows でも動く）。
order_dir = DATA_STORAGE / "order_data"
order_dir

# glob(pattern):
#   ワイルドカードに一致するファイルパスを列挙する。イテレータだが sorted(list(...)) で並べ替えて使う。
# パターンを絞ることで、意図しない CSV を読み込まないようにする。
paths_orders = sorted(order_dir.glob("order_data_2022_12_*.csv"))
if not paths_orders:
    raise FileNotFoundError(
        f"order_data の CSV が見つかりません: {order_dir} / order_data_2022_12_*.csv"
    )

# リスト内包表記: 各パスを read_csv で DataFrame にし、リストに格納する。
# 文字コードはデフォルト UTF-8 想定（教材 CSV に合わせる）。
dfs_orders = [pd.read_csv(p) for p in paths_orders]

# concat(..., ignore_index=True):
#   縦に連結し、行インデックスを 0 から振り直す（元ファイルの行番号が残らないようにする）。
df_orders = pd.concat(dfs_orders, ignore_index=True)

# head(): 先頭5行だけ表示（Jupyter 風の確認用。スクリプト実行時は標準出力には出ない場合あり）。
df_orders.head(5)

# %% [markdown]
# # 課題 2: データの重複行を削除

# =============================================================================
# 課題 2: 完全に同一の行（全列一致）を重複として削除する
# =============================================================================
# 課題1の結果 df_orders をそのまま使う（再読込しない＝一貫性を保つ）。
# %%
df_dedup = df_orders.copy()
df_dedup = df_orders.copy()

# 削除前の行数を記録（レポート用）。
n_before = len(df_dedup)
n_before = len(df_dedup)
# duplicated(): 2行目以降の重複を True とするマスク。keep 指定なしなら先頭を残す既定。
dup_mask = df_dedup.duplicated()
n_dup = dup_mask.sum()

# drop_duplicates(): 重複行を落とす。reset_index(drop=True) でインデックスを連番に整える。
df_dedup = df_dedup.drop_duplicates().reset_index(drop=True)
n_after = len(df_dedup)

print(f"削除前: {n_before} 行, 重複行: {n_dup} 行, 削除後: {n_after} 行")
df_dedup.head()

# %% [markdown]
# ※データフレームの行数を求める

# =============================================================================
# （補足セル）統合後 DataFrame の行数だけを表示する
# =============================================================================
# 教材の「行数を求める」演習用。課題1と同じ glob で再度読み込んでいる点に注意（検算用途）。
# %%
paths_orders = sorted((DATA_STORAGE / "order_data").glob("order_data_2022_12_*.csv"))
dfs_orders = [pd.read_csv(p) for p in paths_orders]
df_orders_rowcount = pd.concat(dfs_orders, ignore_index=True)
print("行数:", len(df_orders_rowcount))

# %% [markdown]
# # 課題 3: データをマージする

# =============================================================================
# 課題 3: order_no をキーに left join で3表を横に結合する
# =============================================================================
# left join の意味: 左（注文）の行はすべて残し、右に対応が無いときは欠損 NaN になる。
# %%
orders_m = df_orders.copy()

"""
order_no	time_type	order_state	receive_type	approve_date	created_at	driver_id	delivery_date	latest_delivery_date	accept_date	pickup_date	pass_date	local_area_flag
0	BY534M	1	4	2	2022/12/1 0:00	2022/12/1 0:00	5606.0	2022/12/1 0:53	2022/12/1 1:23	2022/12/1 0:07	2022/12/1 0:23	2022/12/1 0:44	0
1	GV384O	1	4	2	2022/12/1 0:12	2022/12/1 0:03	76304.0	2022/12/1 0:36	2022/12/1 1:06	2022/12/1 0:14	2022/12/1 0:26	2022/12/1 0:48	0
2	WK460E	1	4	2	2022/12/1 0:06	2022/12/1 0:05	17544.0	2022/12/1 0:48	2022/12/1 1:18	2022/12/1 0:13	2022/12/1 0:17	2022/12/1 0:32	0
"""

deliv_dir = DATA_STORAGE / "delivery_status_history_data"
paths_deliv = sorted(deliv_dir.glob("delivery_status_history_data_2022_12_*.csv"))
if not paths_deliv:
    raise FileNotFoundError(f"delivery_status_history の CSV が見つかりません: {deliv_dir}")
dfs_deliv = [pd.read_csv(p) for p in paths_deliv]
df_delivery = pd.concat(dfs_deliv, ignore_index=True)

"""
	created_date	order_no
0	1669820403	NC509T
1	1669820403	QL837B
2	1669820424	JU264A
3	1669820452	AQ492G
4	1669820522	KV408T
"""

drv_dir = DATA_STORAGE / "driver_shop_arrival_history_data"
paths_drv = sorted(drv_dir.glob("driver_shop_arrival_history_data_2022_12_*.csv"))
if not paths_drv:
    raise FileNotFoundError(f"driver_shop_arrival_history の CSV が見つかりません: {drv_dir}")
dfs_drv = [pd.read_csv(p) for p in paths_drv]
df_driver = pd.concat(dfs_drv, ignore_index=True)

"""
driver_id	order_no	shop_arrival_at	created_at
0	38814	NC509T	2022/12/1 0:00	2022/12/1 0:00
1	21510	PV768H	2022/12/1 0:00	2022/12/1 0:00
2	24607	PC248Y	2022/12/1 0:01	2022/12/1 0:01
"""

# merge 1回目: 注文 × 配達ステータス履歴。
# merge 2回目: その結果 × 店舗到着履歴。
# 両方に created_at があるため、pandas は自動で created_at_x / created_at_y にリネームする（衝突回避）。
merged = orders_m.merge(df_delivery, on="order_no", how="left")
merged = merged.merge(df_driver, on="order_no", how="left")
merged.head()

"""
order_no	time_type	order_state	receive_type	approve_date	created_at_x	driver_id_x	delivery_date	latest_delivery_date	accept_date	pickup_date	pass_date	local_area_flag	created_date	driver_id_y	shop_arrival_at	created_at_y
0	BY534M	1	4	2	2022/12/1 0:00	2022/12/1 0:00	5606.0	2022/12/1 0:53	2022/12/1 1:23	2022/12/1 0:07	2022/12/1 0:23	2022/12/1 0:44	0	1.669821e+09	5606.0	2022/12/1 0:16	2022/12/1 0:16
1	GV384O	1	4	2	2022/12/1 0:12	2022/12/1 0:03	76304.0	2022/12/1 0:36	2022/12/1 1:06	2022/12/1 0:14	2022/12/1 0:26	2022/12/1 0:48	0	1.669821e+09	76304.0	2022/12/1 0:25	2022/12/1 0:25
2	WK460E	1	4	2	2022/12/1 0:06	2022/12/1 0:05	17544.0	2022/12/1 0:48	2022/12/1 1:18	2022/12/1 0:13	2022/12/1 0:17	2022/12/1 0:32	0	1.669821e+09	17544.0	2022/12/1 0:15	2022/12/1 0:15
"""

# %% [markdown]
# # 課題 4: データをフィルターして抽出する
# =============================================================================
# 課題 4: 分析対象に絞り込む（ビジネスルールは教材に準拠）
# =============================================================================
# order_state == 4 … 注文完了など、教材で指定の状態。
# receive_type == 2 … デリバリー受取など、教材で指定の受取タイプ。
# 括弧で条件を囲み & で AND。最後に .copy() して SettingWithCopyWarning を避ける。
# %%
filtered = merged[(merged["order_state"] == 4) & (merged["receive_type"] == 2)].copy()
print("マージ後:", len(merged), "行 → フィルター後:", len(filtered), "行")
filtered.head()
"""
order_no	time_type	order_state	receive_type	approve_date	created_at_x	driver_id_x	delivery_date	latest_delivery_date	accept_date	pickup_date	pass_date	local_area_flag	created_date	driver_id_y	shop_arrival_at	created_at_y
0	BY534M	1	4	2	2022/12/1 0:00	2022/12/1 0:00	5606.0	2022/12/1 0:53	2022/12/1 1:23	2022/12/1 0:07	2022/12/1 0:23	2022/12/1 0:44	0	1.669821e+09	5606.0	2022/12/1 0:16	2022/12/1 0:16
1	GV384O	1	4	2	2022/12/1 0:12	2022/12/1 0:03	76304.0	2022/12/1 0:36	2022/12/1 1:06	2022/12/1 0:14	2022/12/1 0:26	2022/12/1 0:48	0	1.669821e+09	76304.0	2022/12/1 0:25	2022/12/1 0:25
2	WK460E	1	4	2	2022/12/1 0:06	2022/12/1 0:05	17544.0	2022/12/1 0:48	2022/12/1 1:18	2022/12/1 0:13	2022/12/1 0:17	2022/12/1 0:32	0	1.669821e+09	17544.0	2022/12/1 0:15	2022/12/1 0:15
"""
# %% [markdown]
# # 課題 5: データを整形する
# =============================================================================
# 課題 5: UNIX 秒・カラム名の整理など、後続処理向けに整形する
# =============================================================================
# %%

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

# 1回目の merge で注文側の created_at が created_at_x にリネームされている場合、
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

# %% [markdown]
# # 課題 6: for文を使ってループ処理をする

# =============================================================================
# 課題 6: 指定した列をまとめて datetime 型に変換する（存在する列のみ）
# =============================================================================
# errors="coerce": 解釈できない値は NaT（欠損日時）にする。解析時に除外しやすい。
# %%
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

df_shape[date_cols].head()
"""
	approve_date	created_at	delivery_date	latest_delivery_date	accept_date	pickup_date	pass_date	shop_arrival_at	created_at_y
0	2022/12/1 0:00	2022/12/1 0:00	2022/12/1 0:53	2022/12/1 1:23	2022/12/1 0:07	2022/12/1 0:23	2022/12/1 0:44	2022/12/1 0:16	2022/12/1 0:16
1	2022/12/1 0:12	2022/12/1 0:03	2022/12/1 0:36	2022/12/1 1:06	2022/12/1 0:14	2022/12/1 0:26	2022/12/1 0:48	2022/12/1 0:25	2022/12/1 0:25
2	2022/12/1 0:06	2022/12/1 0:05	2022/12/1 0:48	2022/12/1 1:18	2022/12/1 0:13	2022/12/1 0:17	2022/12/1 0:32	2022/12/1 0:15	2022/12/1 0:15
"""

# for文で回してdatetime64に変換する
df_loop = df_shape.copy()
for col in date_cols:
    if col in df_loop.columns:
        df_loop[col] = pd.to_datetime(df_loop[col], errors="coerce")

# 変換結果の dtype だけ抜き出して確認（デバッグ・レポート用）。
df_loop.dtypes.loc[[c for c in date_cols if c in df_loop.columns]]
"""
approve_date	datetime64[ns]
created_at	datetime64[ns]
delivery_date	datetime64[ns]
latest_delivery_date	datetime64[ns]
accept_date	datetime64[ns]
pickup_date	datetime64[ns]
pass_date	datetime64[ns]
shop_arrival_at	datetime64[ns]
created_at_y	datetime64[ns]
"""

# %% [markdown]
# # 課題 7: 遅延している注文の割合を集計する
# 要件: `latest_delivery_date` より配達完了（`delivery_date`）が遅れた注文を「遅延」とし、**承認日（approve_date）ごと**に遅延率を集計する。

# =============================================================================
# 課題 7: 遅延フラグと承認日単位の遅延率
# =============================================================================
# 遅延の定義: delivery_date > latest_delivery_date（厳密に「遅い」＝等しくない）。
# 同一 order_no が複数行ある場合は先頭行のみ採用（マージの多重化対策）。
# %%
df_orders_unique = df_loop.drop_duplicates(subset=["order_no"], keep="first").copy()

# 3列いずれか欠損の行は遅延判定不能のため除外する。
_delay_required = ["approve_date", "delivery_date", "latest_delivery_date"]
df_delay_base = df_orders_unique.dropna(subset=_delay_required).copy()

df_delay_base["is_delayed"] = df_delay_base["delivery_date"] > df_delay_base["latest_delivery_date"]
# normalize(): 時刻を 00:00:00 にそろえた日付（同日のグルーピング用）。
df_delay_base["approve_date_day"] = df_delay_base["approve_date"].dt.normalize()

# groupby.agg: 日付ごとに件数と遅延件数を集計。
# assign で delay_rate を追加（pandas のチェインスタイル）。
df_delay_daily = (
    df_delay_base.groupby("approve_date_day", dropna=False)
    .agg(n_orders=("order_no", "count"), n_delayed=("is_delayed", "sum"))
    .assign(delay_rate=lambda d: (d["n_delayed"] / d["n_orders"]).round(4))
    .reset_index()
    .rename(columns={"approve_date_day": "承認日"})
)

print("【課題7】承認日別 遅延率（delay_rate = n_delayed / n_orders）")
print(df_delay_daily.to_string(index=False))
print(f"遅延注文数（全体）: {df_delay_base['is_delayed'].sum()} / {len(df_delay_base)}")

# %% [markdown]
# # 課題 8: 遅延の要因分析
# 遅延注文に限定し、各工程の所要時間（時間）を算出して要約する。ボトルネックは「最も時間が長かった工程」を注文ごとに特定し件数集計する。

# =============================================================================
# 課題 8: 遅延注文のみを対象に工程間の経過時間を分解する
# =============================================================================
# 工程の区切りは教材データの列名に合わせた典型的なフロー:
#   承認 → 受付 → ピックアップ → 通過 → 配達完了
# 店舗到着時刻がある場合は先頭に「店舗到着〜受付」を挿入する。
# %%


def _stage_hours(from_s: pd.Series, to_s: pd.Series) -> pd.Series:
    """
    終了時刻 - 開始時刻 を「時間」単位の float Series で返す。

    注意:
        時刻が逆転していると負の値になる（データ不整合や定義の見直しの手がかり）。
    """
    return (to_s - from_s).dt.total_seconds() / 3600.0


df_delayed = df_delay_base[df_delay_base["is_delayed"]].copy()

# (新しい列名, 開始列, 終了列) のタプルリスト。後から insert で順序を調整可能。
_stage_defs = [
    ("h_承認〜受付", "approve_date", "accept_date"),
    ("h_受付〜ピックアップ", "accept_date", "pickup_date"),
    ("h_ピックアップ〜通過", "pickup_date", "pass_date"),
    ("h_通過〜配達完了", "pass_date", "delivery_date"),
]
if "shop_arrival_at" in df_delayed.columns:
    _stage_defs.insert(0, ("h_店舗到着〜受付", "shop_arrival_at", "accept_date"))

stage_col_names: list[str] = []
for col_name, c_from, c_to in _stage_defs:
    if c_from in df_delayed.columns and c_to in df_delayed.columns:
        df_delayed[col_name] = _stage_hours(df_delayed[c_from], df_delayed[c_to])
        stage_col_names.append(col_name)

# 工程ごとに記述統計を1行ずつ dict で溜め、最後に DataFrame 化する。
_rows = []
for c in stage_col_names:
    s = df_delayed[c].dropna()
    if s.empty:
        continue
    label = c.replace("h_", "").replace("_", "〜")
    _rows.append(
        {
            "工程": label,
            "平均_h": round(float(s.mean()), 3),
            "中央値_h": round(float(s.median()), 3),
            "p90_h": round(float(s.quantile(0.9)), 3),
            "負の値件数": int((s < 0).sum()),
            "件数": int(s.shape[0]),
        }
    )
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
_detail_cols = (
    [
        "order_no",
        "is_delayed",
        "approve_date",
        "latest_delivery_date",
        "delivery_date",
        "bottleneck_stage",
        "bottleneck_stage_label",
    ]
    + stage_col_names
)
_detail_cols = [c for c in _detail_cols if c in df_delayed.columns]
df_delayed_export = df_delayed[_detail_cols].copy()

# %% [markdown]
# # 課題 9: スプレッドシートへのデータ抽出
# 課題7・8の集計をスプレッドシート「Update」に書き込む（gspread）。併せて `output` フォルダに Excel（.xlsx）へ保存する。
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
        df_delayed_export.to_excel(writer, sheet_name="課題8_遅延明細", index=False)
    print(f"Excel 出力しました: {EXCEL_OUTPUT_PATH.resolve()}")
except ImportError:
    print("openpyxl が未インストールです。pip install openpyxl を実行してください。")
    raise

# --- gspread 更新処理 ---
try:
    import gspread
    from google.auth import default as google_auth_default

    if "google.colab" in sys.modules:
        from google.colab import auth as colab_auth

        colab_auth.authenticate_user()
        _creds, _ = google_auth_default()
        _gc = gspread.authorize(_creds)
        
        # スプレッドシートを開く（IDが正しいか、形式がスプレッドシートか確認済み前提）
        _sh = _gc.open_by_key(SPREADSHEET_ID)
        
        try:
            _ws = _sh.worksheet(WORKSHEET_TITLE)
        except gspread.WorksheetNotFound:
            _ws = _sh.add_worksheet(title=WORKSHEET_TITLE, rows=5000, cols=30)

        _matrix = build_update_sheet_values()
        _ws.clear()
        
        # 値の書き込み（Timestampを文字列化したマトリックスを使用）
        try:
            _ws.update(values=_matrix, range_name="A1", value_input_option="USER_ENTERED")
        except TypeError:
            # 古いバージョンのgspread用のフォールバック
            _ws.update("A1", _matrix, value_input_option="USER_ENTERED")
            
        print(f"gspread: スプレッドシート「{WORKSHEET_TITLE}」を更新しました。")
    else:
        print("gspread スキップ: Colab 環境ではありません。")
except Exception as e:
    print(f"gspread 更新中にエラーが発生しました: {e}")

# %% [markdown]
# ## 追加: データ集計・DataStorage を使った簡易分析
#
# 前提: 上記の `df_dedup`, `merged`, `df_loop` まで実行済み。

# =============================================================================
# 追加セル: 教材データのざっくりした分布確認（提出必須ではない場合あり）
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
    tmp["lead_min"] = (tmp["accept_date"] - tmp["shop_arrival_at"]).dt.total_seconds() / 60.0
    print("【5】店舗到着〜受付までの分数（分）※負の値はデータ不整合の可能性")
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
