defmodule Clickhousex.Codec.JSONTest do
  use ExUnit.Case
  alias Clickhousex.Codec.JSON

  @dummy_json """
  {
    "data":
    [
        [
            [
                "mytype",
                "EUR",
                [
                    {
                        "commission": 1.1,
                        "lower_limit": -9999999999.99999999,
                        "upper_limit": 100
                    },
                    {
                        "commission": 2.2,
                        "lower_limit": 100,
                        "upper_limit": 200
                    }
                ],
                null,
                1.01,
                1,
                null,
  "2001-01-01 00:00:00"
            ]
        ]
    ],
    "meta":
    [
        {
            "name": "dictGet('mydict', ('type', 'currency', 'tiers', 'null_dec_field', 'dec_field', 'int_field', 'null_int_field'), 57)",
            "type": "Tuple(type String, currency String, tiers Array(Map(String, Decimal(18, 8))), null_dec_field Nullable(Decimal(18, 8)), dec_field Decimal(18, 8), int_field Int64, null_int_field Nullable(UInt64), datetime_field DateTime"
        }
    ],
    "rows": 1,
    "statistics":
    {
        "bytes_read": 1,
        "elapsed": 0.0008828,
        "rows_read": 1
    }
  }
  """

  test "able to decode a complex json" do
    assert {:ok, decoded} = JSON.decode(@dummy_json)
    assert decoded.count == 1
    assert [[row]] = decoded.rows
    assert row["type"] == "mytype"
    assert row["currency"] == "EUR"
    assert is_nil(row["null_dec_field"])
    assert Decimal.eq?(row["dec_field"], Decimal.new("1.01"))
    assert row["int_field"] == 1
    assert is_nil(row["null_int_field"])
    dt = DateTime.new!(~D[2001-01-01], ~T[00:00:00], "Etc/UTC") |> DateTime.to_naive()
    assert row["datetime_field"] == dt

    assert [tier1, tier2] = row["tiers"]
    assert Decimal.eq?(tier1["commission"], Decimal.new("1.1"))
    assert Decimal.eq?(tier1["lower_limit"], Decimal.new("-9999999999.99999999"))
    assert Decimal.eq?(tier1["upper_limit"], Decimal.new(100))
    assert Decimal.eq?(tier2["commission"], Decimal.new("2.2"))
    assert Decimal.eq?(tier2["lower_limit"], Decimal.new(100))
    assert Decimal.eq?(tier2["upper_limit"], Decimal.new(200))
  end
end
