from random import randint

import dagster as dg
import polars as pl

from lerpz_data.invoice import (
    Invoice,
    invoice,
    SourceList,
)
from lerpz_data.transform import (
    collect,
    Transform,
    transform,
    TransformData,
)


def load_data(sources: SourceList) -> TransformData:
    return TransformData(
        {
            sources[0]: pl.LazyFrame(
                {
                    "item": ["item1", "item2", "item3", "item4"],
                }
            ),
            sources[1]: pl.LazyFrame(
                {
                    "item": ["item1", "item2", "item3", "item4"],
                    "price": [randint(1, 100) for _ in range(4)],
                }
            ),
            sources[2]: pl.LazyFrame(
                {
                    "item": ["item1", "item2", "item3", "item4"],
                    "quantity": [randint(1, 10) for _ in range(4)],
                }
            ),
        }
    )


@dg.asset
@invoice("lerpz_cost")
def create_invoice(sources: SourceList = ["items", "mapping/cost", "mapping/quantity"]) -> Invoice:
    data = load_data(sources)

    return Invoice.from_transform(
        Transform.builder(data)
        .add_rule(calculate_cost)
        .add_rule(join_quanitty)
        .finish(collect_invoice)
    )


@transform()
def calculate_cost(data: TransformData) -> TransformData:
    """Calculate the cost of the invoice."""
    data["items"] = data["items"].join(data["mapping/cost"], on="item")
    del data["mapping/cost"]

    rate = 2.0  # Just a fake currecy rate
    data["items"] = data["items"].with_columns(price=(pl.col("price") * rate))
    return data


@transform()
def join_quanitty(data: TransformData) -> TransformData:
    """Join the quantity data with the cost data."""
    data["items"] = data["items"].join(data["mapping/quantity"], on="item")
    del data["mapping/quantity"]
    return data


@collect()
def collect_invoice(data: TransformData) -> pl.DataFrame:
    """Collect the invoice data."""
    return data["items"].collect()


defs = dg.Definitions(assets=[create_invoice])
