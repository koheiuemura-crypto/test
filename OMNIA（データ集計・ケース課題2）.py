# -*- coding: utf-8 -*-
"""
OMNIA（データ集計・ケース課題2）
元ノート: OMNIA（データ集計・ケース課題2）.ipynb から複製・回答用。
データ: データ集計・DataStorage 配下の order_data / delivery_status_history_data / driver_shop_arrival_history_data
"""

# %%
# ライブラリのインポート
import pandas as pd
from pathlib import Path
from google.colab import drive

drive.mount("/content/drive")


# %%
def resolve_data_storage() -> Path:
    """データ集計・DataStorage フォルダを特定（Colab / ローカル）。"""
    candidates: list[Path] = []
    try:
        candidates.append(Path(__file__).resolve().parent / "データ集計・DataStorage")
    except NameError:
        pass
    drive_root = Path("/content/drive/MyDrive")
    if drive_root.exists():
        candidates.extend(
            [
                drive_root / "OMNIA/考える/アナリティクス/データ集計・DataStorage",
                drive_root / "OMNIA/アナリティクス/データ集計・DataStorage",
            ]
        )
    candidates.append(Path("データ集計・DataStorage"))
    candidates.append(Path.cwd() / "データ集計・DataStorage")
    for c in candidates:
        if c.is_dir():
            return c.resolve()
    raise FileNotFoundError(
        "データ集計・DataStorage が見つかりません。試したパス:\n"
        + "\n".join(f"  - {p}" for p in candidates)
    )


DATA_STORAGE = resolve_data_storage()

# %% [markdown]
# # 課題 1: 複数の日付のデータを一つのデータフレームにまとめる

# %%
# CSVファイルのパスを指定
order_dir = DATA_STORAGE / "order_data"
paths_orders = sorted(order_dir.glob("order_data_2022_12_*.csv"))

# 各CSVファイルを読み込み、リストに格納
dfs_orders = [pd.read_csv(p) for p in paths_orders]

# データフレームを統合
df_orders = pd.concat(dfs_orders, ignore_index=True)

# 統合したデータフレームの先頭5行を表示
df_orders.head()

# %% [markdown]
# # 課題 2: データの重複行を削除

# %%
# CSVファイルのパスを指定（課題1と同じ order データを利用）
# CSVファイルを読み込む → すでに df_orders があるのでそれを使う
df_dedup = df_orders.copy()

# 重複行の確認
n_before = len(df_dedup)
dup_mask = df_dedup.duplicated()
n_dup = dup_mask.sum()

# 重複行を削除
df_dedup = df_dedup.drop_duplicates().reset_index(drop=True)
n_after = len(df_dedup)

# 結果を確認
print(f"削除前: {n_before} 行, 重複行: {n_dup} 行, 削除後: {n_after} 行")
df_dedup.head()

# %% [markdown]
# ※データフレームの行数を求める

# %%
# 課題1と同様に読み込み・統合して行数を取得
paths_orders = sorted((DATA_STORAGE / "order_data").glob("order_data_2022_12_*.csv"))
dfs_orders = [pd.read_csv(p) for p in paths_orders]
df_orders_rowcount = pd.concat(dfs_orders, ignore_index=True)
print("行数:", len(df_orders_rowcount))

# %% [markdown]
# # 課題 3: データをマージする

# %%
# 1つ目: order_data（課題1で統合済みの df_orders を使用）
orders_m = df_orders.copy()

# 2つ目: delivery_status_history_data
deliv_dir = DATA_STORAGE / "delivery_status_history_data"
paths_deliv = sorted(deliv_dir.glob("delivery_status_history_data_2022_12_*.csv"))
dfs_deliv = [pd.read_csv(p) for p in paths_deliv]
df_delivery = pd.concat(dfs_deliv, ignore_index=True)

# 3つ目: driver_shop_arrival_history_data
drv_dir = DATA_STORAGE / "driver_shop_arrival_history_data"
paths_drv = sorted(drv_dir.glob("driver_shop_arrival_history_data_2022_12_*.csv"))
dfs_drv = [pd.read_csv(p) for p in paths_drv]
df_driver = pd.concat(dfs_drv, ignore_index=True)

# 3種類のデータフレームをマージ（キーは order_no）
# ※注文と店舗到着履歴に両方 created_at があるため、2回目のマージ後は created_at_x / created_at_y などになる
merged = orders_m.merge(df_delivery, on="order_no", how="left")
merged = merged.merge(df_driver, on="order_no", how="left")
merged.head()

# %% [markdown]
# # 課題 4: データをフィルターして抽出する

# %%
# ここから、課題3で作成したマージ後のデータフレームを使う
# 例: 注文完了（order_state == 4）かつデリバリー（receive_type == 2）に絞る
filtered = merged[(merged["order_state"] == 4) & (merged["receive_type"] == 2)].copy()
print("マージ後:", len(merged), "行 → フィルター後:", len(filtered), "行")
filtered.head()

# %% [markdown]
# # 課題 5: データを整形する

# %%
# 必要なライブラリは先頭でインポート済み（datetime は pandas が利用）


# UNIXTIME（秒）を日時に変換する関数を作成
def unix_to_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, unit="s", errors="coerce")


df_shape = filtered.copy()
if "created_date" in df_shape.columns:
    df_shape["created_date"] = unix_to_datetime(df_shape["created_date"])

# カラム名の変更 (created_at_x → created_at) ※マージで _x が付いた場合のみ
if "created_at_x" in df_shape.columns:
    df_shape = df_shape.rename(columns={"created_at_x": "created_at"})

df_shape.head()

# %% [markdown]
# # 課題 6: for文を使ってループ処理をする

# %%
# 日付型に変換したいカラムのリストを指定（存在するものだけ処理）
date_cols = [
    "approve_date",
    "created_at",
    "delivery_date",
    "latest_delivery_date",
    "accept_date",
    "pickup_date",
    "pass_date",
    "shop_arrival_at",
    "created_at_y",
]

df_loop = df_shape.copy()
for col in date_cols:
    if col in df_loop.columns:
        df_loop[col] = pd.to_datetime(df_loop[col], errors="coerce")

df_loop.dtypes.loc[[c for c in date_cols if c in df_loop.columns]]

# %% [markdown]
# # 課題 9: スプレッドシートへのデータ抽出

# %%
# 必要なライブラリは pandas で十分（CSVで出力し、Google スプレッドシートに「ファイルを開く」で取り込み可能）
export_path = DATA_STORAGE / "課題2_export_for_spreadsheet.csv"
df_loop.to_csv(export_path, index=False, encoding="utf-8-sig")
print(f"出力しました: {export_path}")
print("Google スプレッドシートでは「ファイル」→「インポート」→この CSV を指定してください。")

# gspread で直接書き込む場合は認証設定が必要です（講義の手順に従ってください）
# import gspread
# from google.colab import auth
# auth.authenticate_user()

# %% [markdown]
# ## 追加: データ集計・DataStorage を使った簡易分析
#
# 前提: 上記の `df_dedup`, `merged`, `df_loop` まで実行済み。

# %%
# --- 分析: order_data（4日分・重複除去後）とマージ結果 ---

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
