# -*- coding: utf-8 -*-
# %%
from google.colab import drive

drive.mount("/content/drive")

# %% [markdown]
# ## 問題1
#
# - 施策Aで購入したユーザーIDリスト（`user_ids_a`）と、施策Bで購入したユーザーIDリスト（`user_ids_b`）があります。
#
# - 後日、施策Aのみで購入したユーザーには300円クーポン、施策Bのみで購入したユーザーには500円クーポン、両方の施策で購入したユーザーには750円クーポンを配布することになっています。

# %%
user_ids_a = {1, 3, 4, 6, 7}
user_ids_b = {1, 4, 7, 9, 10}

# %% [markdown]
# ### 問題1-1
#
# - クーポン配布のための金額ごとのユーザーIDリストを作成して下さい。

# %%
# 300円クーポン配布（施策Aのみ＝AにあってBにない）
users_300 = user_ids_a - user_ids_b
users_300

# %%
# 500円クーポン配布（施策Bのみ＝BにあってAにない）
users_500 = user_ids_b - user_ids_a
users_500

# %%
# 750円クーポン配布（両施策で購入＝積集合）
users_750 = user_ids_a & user_ids_b
users_750

# %% [markdown]
# ### 問題1-2
#
# - コストの合計金額を求めて下さい。

# %%
# 300円クーポン配布コスト
cost_300 = len(users_300) * 300
cost_300

# %%
# 500円・750円クーポン配布コスト（見出しは750のみのため両方ここで算出）
cost_500 = len(users_500) * 500
cost_750 = len(users_750) * 750
cost_500, cost_750

# %%
cost_300, cost_750

# %%
# 配布コスト計
cost_300 + cost_500 + cost_750

# %% [markdown]
# ## 問題2
#
# - 前日までの既存ユーザーのリスト（`user_ids_existing`）と、今日の注文データの辞書（`join_orders`）があります。
#
# ```
# join_orders = {
#   オーダーID: {
#     "user_id": ユーザーID,
#     "receive_type": 1=テイクアウト、2=デリバリー,
#     "item_total_price": 商品金額の合計,
#     "discount_price": クーポン金額
#   }
# }
# ```

# %%
user_ids_existing = {1, 3, 5, 7, 9, 11}

join_orders = {
    1: {
        "user_id": 13,
        "receive_type": 2,
        "item_total_price": 2400,
        "discount_price": 300,
    },
    2: {
        "user_id": 1,
        "receive_type": 1,
        "item_total_price": 2600,
        "discount_price": 500,
    },
    3: {
        "user_id": 3,
        "receive_type": 1,
        "item_total_price": 2100,
        "discount_price": 0,
    },
    4: {
        "user_id": 9,
        "receive_type": 2,
        "item_total_price": 3000,
        "discount_price": 1000,
    },
    5: {
        "user_id": 12,
        "receive_type": 2,
        "item_total_price": 5550,
        "discount_price": 800,
    },
    6: {
        "user_id": 5,
        "receive_type": 2,
        "item_total_price": 2700,
        "discount_price": 0,
    },
    7: {
        "user_id": 8,
        "receive_type": 1,
        "item_total_price": 3100,
        "discount_price": 500,
    },
    8: {
        "user_id": 5,
        "receive_type": 1,
        "item_total_price": 3000,
        "discount_price": 600,
    },
    9: {
        "user_id": 4,
        "receive_type": 2,
        "item_total_price": 2400,
        "discount_price": 300,
    },
    10: {
        "user_id": 6,
        "receive_type": 2,
        "item_total_price": 2800,
        "discount_price": 0,
    },
}

# %% [markdown]
# #### 問題2-1
#
# - 今日の既存購入率を求めて下さい。

# %%
# 今日の購入者リストを作成
buyer_ids = {o["user_id"] for o in join_orders.values()}
# buyer_ids [o["user_id"] for o in join_orders.values()]　
# len(buyer_ids)

# 既存ユーザーと新規ユーザーに分ける
existing_buyers = buyer_ids & user_ids_existing
new_buyers = buyer_ids - user_ids_existing

# 既存ユーザーの購入数を計算（注文件数）
existing_order_count = sum(1 for o in join_orders.values() if o["user_id"] in user_ids_existing)

# 新規ユーザーの購入数を計算（注文件数）
new_order_count = sum(1 for o in join_orders.values() if o["user_id"] not in user_ids_existing)

# 既存ユーザーの購入率（既存ユーザー母数に対する、当日1回以上購入したユニーク人数）
existing_purchase_rate = len(existing_buyers) / len(user_ids_existing)

# 今日の購入者セット
buyer_ids, existing_buyers, existing_purchase_rate

# %%
# 既存ユーザー数（母数）
len(user_ids_existing)

# 当日ユニーク購入者数
len(buyer_ids)

# %%
# まとめ: 既存購入率 = 当日購入したユニーク既存ユーザー数 / 既存ユーザー数
print(f"既存ユーザー数: {len(user_ids_existing)}")
print(f"当日購入ユニーク既存ユーザー数: {len(existing_buyers)}")
print(f"今日の既存購入率: {existing_purchase_rate:.4f}")

# %% [markdown]
# #### 問題2-2
#
# - 今日のデリバリー注文のみの既存定価購入率を求めて下さい。

# %%
# 今日の購入者のうちデリバリー（receive_type==2）のみ
buyer_ids_delivery = {o["user_id"] for o in join_orders.values() if o["receive_type"] == 2}
existing_delivery_buyers = buyer_ids_delivery & user_ids_existing
buyer_ids_delivery, existing_delivery_buyers

# %%
# デリバリーかつ既存ユーザーの注文に限定し、「定価購入」= クーポン割引なし（discount_price==0）の割合
delivery_existing_orders = [
    o for o in join_orders.values() if o["receive_type"] == 2 and o["user_id"] in user_ids_existing
]
teika_orders = [o for o in delivery_existing_orders if o["discount_price"] == 0]
n_del = len(delivery_existing_orders)
n_teika = len(teika_orders)
rate_teika = n_teika / n_del if n_del else 0.0
print(f"デリバリーかつ既存の注文数: {n_del}, うち定価（discount_price=0）: {n_teika}")
print(f"今日のデリバリー注文のみの既存定価購入率: {rate_teika:.4f}")

# %%
# （空セル相当）

# %% [markdown]
# ## 問題3
# - 本問題では、還元祭のクーポン付与のロジックを組んでいきます。最終的には、どのユーザーに、どれくらいの額のクーポンを付与するか、そのようなクーポンコードになるか、をまとめた、`user_id`、`discount_price`、`discount_code`の3列だけのcsvファイルを出力して下さい。
#   - 以下が今回の対象注文条件です。
#     - 商品金額（`item_total_price`）が1500円以上
#     - 予約注文を除く（`time_type=1`）
#     - デリバリー注文（`receive_type=2`）
#     - 注文完了している（`order_state=4`）
#     - キャンペーンは11時から開始したので、初日分は11時以降に注文されたもののみに絞る。
#
#   - 付与するクーポン金額の計算式
#     - 配達料無料クーポン（`FREE-`から始まる）を使用している注文の場合は、`item_total_price - item_discount_price`
#     - それ以外の場合は、`item_total_price - item_discount_price - discount_price`
#     - 還元率は30%。
#
#   - 発行するクーポンコードの規則は、`DELI-HALF-(order_no)`にします。
#
# - 対象期間は、`2021/9/25〜9/28`で行って下さい。
#
# - この問題では、driveにある`OMNIA/考える/アナリティクス/データ集計・DataStorage/`の中にある`join_orders`のデータを使って下さい。

# %%
# ヒント: 以下のライブラリを使って解いてみて下さい

import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path


def resolve_join_orders_dir() -> Path:
    """join_orders_2021_09_*.csv があるディレクトリを特定する（Colab / ローカル / CWD 差を吸収）。"""

    def has_target_csv(d: Path) -> bool:
        return d.is_dir() and any(d.glob("join_orders_2021_09_*.csv"))

    candidates: list[Path] = []
    try:
        _here = Path(__file__).resolve().parent
        candidates.append(_here / "データ集計・DataStorage/join_orders")
    except NameError:
        pass

    drive = Path("/content/drive/MyDrive")
    if drive.exists():
        candidates.extend(
            [
                drive / "OMNIA/考える/アナリティクス/データ集計・DataStorage/join_orders",
                drive / "OMNIA/アナリティクス/データ集計・DataStorage/join_orders",
            ]
        )

    candidates.append(Path("データ集計・DataStorage/join_orders"))
    candidates.append(Path.cwd() / "データ集計・DataStorage/join_orders")

    for d in candidates:
        if has_target_csv(d):
            return d.resolve()

    msg = "join_orders_2021_09_*.csv が見つかりません。次のいずれかにデータを置くか、パスを追加してください:\n"
    msg += "\n".join(f"  - {d}" for d in candidates)
    raise FileNotFoundError(msg)

# %%
# 問題3: 還元祭クーポン（対象注文を抽出し、注文ごとに付与額とコードを算出）

join_dir = resolve_join_orders_dir()
paths = sorted(join_dir.glob("join_orders_2021_09_*.csv"))
df = pd.concat(
    [pd.read_csv(p, sep="\t", dtype={"order_no": str}) for p in paths],
    ignore_index=True,
)

for col in ["item_total_price", "item_discount_price", "discount_price"]:
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

df["order_dt"] = pd.to_datetime(df["order_date"])
mask_period = (df["order_dt"] >= "2021-09-25 11:00:00") & (df["order_dt"] <= "2021-09-28 23:59:59")

# 課題文は「予約を除く（time_type=1）」とあるが、提供CSVでは即時注文がほぼ time_type=1。
# 予約らしき行は time_type=2 のため、ここでは「予約除外 = time_type != 2」とした（講義の定義に合わせて要確認）。
mask_not_reserved = df["time_type"] != 2

dc = df["discount_code"].fillna("").astype(str)
mask = (
    mask_period
    & mask_not_reserved
    & (df["item_total_price"] >= 1500)
    & (df["order_state"] == 4)
    & (df["receive_type"] == 2)
)
df_f = df.loc[mask].copy()

is_free = dc.loc[df_f.index].str.startswith("FREE-", na=False)
base = df_f["item_total_price"] - df_f["item_discount_price"]
base = base - df_f["discount_price"].where(~is_free, 0)

df_f["discount_price"] = (base * 0.3).round().astype(int)
df_f["discount_code"] = "DELI-HALF-" + df_f["order_no"].astype(str)
out = df_f[["user_id", "discount_price", "discount_code"]].copy()
out["user_id"] = pd.to_numeric(out["user_id"], errors="coerce").astype("Int64")

out_csv = join_dir / "repay_festival_coupons.csv"
out.to_csv(out_csv, index=False, encoding="utf-8-sig")
print(f"出力: {out_csv}  行数: {len(out)}")
out.head()

# %%
# （空セル相当）

# %% [markdown]
# ## 問題4
# - 今回の課題
#   - area_idは、以下の2種類があります。
#     - shop_delivery_area_id（その注文の店舗があるエリア）
#     - user_delivery_area_id（その注文のユーザーがいるエリア）
#   - ユーザーチームとクルーチームが横断するプロジェクトにおいて、各エリアごとのデイリー平均注文数をデータとして持っていく必要があるので、（以下の定義に則り）データを集計してください。
#
# - 今回のデータ定義：ユーザーチームで使っている「注文数」とクルーチームが使っている「注文数」は微妙に異なります。
#   - ユーザーチームは、ユーザーの位置を基に、地域ごとの施策を検討するため、ユーザーの位置をベースとして注文数を集計する。
#   - クルーチームは、クルーをいかに店舗の位置の近くに寄せられるか、ということが大事である（正確に言えば、「注文数が増えてクルーを確保する必要がある」という文脈においての注文数は、店舗の位置ベースで考える必要がある）ので、店舗の位置ベースで注文数を集計する。
#
# - 対象期間は、`2021/9/25〜9/28`で行って下さい。
#
# - この問題では、driveにある`OMNIA/考える/アナリティクス/データ集計・DataStorage/`の中にある`join_orders`のデータを使って下さい。
#
# - 補足
#     - この問題では、データの定義確認を学んでもらいます。
#     - menu内でも、各チームの考えているデータ定義が異なるので、他チームからデータ抽出の依頼を受けたときや情報を連携する際は必ず、データ定義を確認してから、集計するようにしましょう。

# %%
# 問題4: エリア別「デイリー平均注文数」（ユーザーチーム＝ユーザー位置、クルーチーム＝店舗位置）
# （resolve_join_orders_dir は上の「ヒント」セルで定義）

join_dir = resolve_join_orders_dir()
paths = sorted(join_dir.glob("join_orders_2021_09_*.csv"))
df4 = pd.concat([pd.read_csv(p, sep="\t") for p in paths], ignore_index=True)

df4["order_dt"] = pd.to_datetime(df4["order_date"])
df4 = df4[(df4["order_dt"] >= "2021-09-25") & (df4["order_dt"] < "2021-09-29")].copy()
df4["order_day"] = df4["order_dt"].dt.date

# ユーザーチーム: user_delivery_area_id 基準で日別件数 → エリアごとの日次件数の平均
u_daily = df4.groupby(["user_delivery_area_id", "order_day"]).size().reset_index(name="n_orders")
user_team_avg = u_daily.groupby("user_delivery_area_id")["n_orders"].mean().reset_index(name="daily_avg_orders")

# クルーチーム: shop_delivery_area_id 基準
c_daily = df4.groupby(["shop_delivery_area_id", "order_day"]).size().reset_index(name="n_orders")
crew_team_avg = c_daily.groupby("shop_delivery_area_id")["n_orders"].mean().reset_index(name="daily_avg_orders")

user_team_avg.to_csv(join_dir / "daily_avg_orders_user_team.csv", index=False, encoding="utf-8-sig")
crew_team_avg.to_csv(join_dir / "daily_avg_orders_crew_team.csv", index=False, encoding="utf-8-sig")

user_team_avg.head(10), crew_team_avg.head(10)

# %%
# 両チームの集計結果の行数・欠損の確認例
print("ユーザーチーム（user_delivery_area_id）エリア数:", len(user_team_avg))
print("クルーチーム（shop_delivery_area_id）エリア数:", len(crew_team_avg))
