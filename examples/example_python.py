from random import randint
from lerpz_invoice import BuildType
from lerpz_invoice.product import ProductBuilder
import polars as pl
from lerpz_invoice.invoice import InvoiceData, SourceList, invoice
from lerpz_invoice.transform import collect, transform


def load_data(sources: SourceList) -> InvoiceData:
    return InvoiceData({
        sources[0]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
        }),
        sources[1]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
            "price": [randint(1, 100) for _ in range(4)]
        }),
        sources[2]: pl.LazyFrame({
            "item": ["item1", "item2", "item3", "item4"],
            "quantity": [randint(1, 10) for _ in range(4)]
        })
    })


@invoice("lerpz_cost")
def create_invoice(sources: SourceList = ["items", "mapping/cost", "mapping/quantity"]):
    data = load_data(sources)
    product_builder = (ProductBuilder(data)
        .invoice(lambda t :
            t.add_rule(calculate_cost)
             .add_rule(join_quanitty)
             .finish(collect_invoice)
        )
        .correction(lambda c :
            c.add_rule(calculate_cost)
             .add_rule(join_quanitty)
             .finish(collect_invoice)
        )
        .visualize(lambda c :
            c.add_rule(calculate_cost)
             .add_rule(join_quanitty)
             .finish(collect_invoice)
        )
    )

    invoice = product_builder.build(BuildType.INVOICE)
    correction = product_builder.build(BuildType.CORRECTION)
    visualize = product_builder.build(BuildType.VISUALIZE)

    return (invoice, correction, visualize)


@transform()
def calculate_cost(data: InvoiceData) -> InvoiceData:
    """Calculate the cost of the invoice."""
    data["items"] = data["items"].join(data["mapping/cost"], on="item")
    del data["mapping/cost"]

    rate = 1.0 # Just a fake currecy rate
    data["items"] = data["items"].with_columns(
        price=(pl.col("price")  * rate)
    )
    return data


@transform()
def join_quanitty(data: InvoiceData) -> InvoiceData:
    """Join the quantity data with the cost data."""
    data["items"] = data["items"].join(data["mapping/quantity"], on="item")
    del data["mapping/quantity"]
    return data


@collect()
def collect_invoice(data: InvoiceData) -> pl.DataFrame:
    """Collect the invoice data."""
    return data["items"].collect()


if __name__ == "__main__":
    create_invoice()
