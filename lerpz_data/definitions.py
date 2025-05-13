from random import randint

import dagster as dg
import polars as pl

from lerpz_data.invoice import (
    Invoice,
    SourceList,
)
from lerpz_data.transform import (
    Transform,
    TransformData,
)


@dg.asset(compute_kind="python")
def mock_data() -> TransformData:
    sources: SourceList = ["items", "mapping/cost", "mapping/quantity"]

    return TransformData({
        sources[0]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
        }),
        sources[1]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
            "price": [randint(1, 100) for _ in range(4)],
        }),
        sources[2]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
            "quantity": [randint(1, 10) for _ in range(4)],
        }),
    })


@dg.asset(compute_kind="python")
def create_invoice(mock_data: TransformData) -> pl.DataFrame:
    invoice = Invoice.from_transform(
        Transform.builder(mock_data)
        .add_rule(calculate_cost)
        .add_rule(join_quanitty)
        .finish(collect_invoice)
    )

    return invoice.data()


def calculate_cost(data: TransformData) -> TransformData:
    """Calculate the cost of the invoice.

    Returns:
        Data with items joined with cost.
    """
    data["items"] = data["items"].join(data["mapping/cost"], on="item")
    del data["mapping/cost"]

    rate = 2.0  # Just a fake currecy rate
    data["items"] = data["items"].with_columns(price=(pl.col("price") * rate))
    return data


def join_quanitty(data: TransformData) -> TransformData:
    """Join the quantity data with the cost data.

    Returns:
        Data with items joined with quanity.
    """
    data["items"] = data["items"].join(data["mapping/quantity"], on="item")
    del data["mapping/quantity"]
    return data 


def collect_invoice(data: TransformData) -> pl.DataFrame:
    """Collect the invoice data.

    Returns:
        The invoice dataframe.
    """
    return data["items"].collect()


daily_refresh_job = dg.define_asset_job("daily_refresh", selection=[create_invoice])

daily_schedule = dg.ScheduleDefinition(
    job=daily_refresh_job,
    cron_schedule="*/2 * * * *",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

defs = dg.Definitions(
    assets=[create_invoice, mock_data], jobs=[daily_refresh_job], schedules=[daily_schedule]
)
