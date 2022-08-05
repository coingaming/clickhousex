defmodule Clickhousex.Codec.JSON do
  @moduledoc """
  `Clickhousex.Codec` implementation for JSON output format.

  See [JSON][1], [JSONCompact][2].

  [1]: https://clickhouse.tech/docs/en/interfaces/formats/#json
  [2]: https://clickhouse.tech/docs/en/interfaces/formats/#jsoncompact
  """

  @use_decimal Application.compile_env(:clickhousex, :use_decimal, false)
  @jason_opts if @use_decimal, do: [floats: :decimals], else: []
  @types_regex ~r/(?<column>[\w]+) (?<type>[\w]+\([\w0-9\(, ]*\)*|[\w]+)|(?<nameless_type>[\w]+\([\w0-9\(, ]*\)*|[\w]+)/m

  alias Clickhousex.Codec
  @behaviour Codec

  @impl Codec
  defdelegate encode(query, replacements, params), to: Codec.Values

  @impl Codec
  def request_format do
    "Values"
  end

  @impl Codec
  def response_format do
    "JSONCompact"
  end

  @impl Codec
  def new do
    []
  end

  @impl Codec
  def append(state, data) do
    [state, data]
  end

  @impl Codec
  def decode(response) do
    case Jason.decode(response, @jason_opts) do
      {:ok, %{"meta" => meta, "data" => data, "rows" => row_count}} ->
        column_names = Enum.map(meta, & &1["name"])
        column_types = Enum.map(meta, & &1["type"])

        rows =
          for row <- data do
            for {raw_value, column_type} <- Enum.zip(row, column_types) do
              to_native(column_type, raw_value)
            end
          end

        {:ok, %{column_names: column_names, rows: rows, count: row_count}}

      _ ->
        {:ok, %{column_names: [], rows: [], count: 0}}
    end
  end

  defp to_native(_, nil) do
    nil
  end

  defp to_native(<<"Nullable(", type::binary>>, value) do
    type = String.replace_suffix(type, ")", "")
    to_native(type, value)
  end

  defp to_native(<<"Array(", type::binary>>, value) do
    type = String.replace_suffix(type, ")", "")
    Enum.map(value, &to_native(type, &1))
  end

  defp to_native(<<"Tuple(", types::binary>>, value) do
    types
    |> String.replace_suffix(")", "")
    |> then(&Regex.scan(@types_regex, &1, capture: :all_names))
    |> Enum.with_index()
    |> Enum.map(fn
      {["", type, ""], index} ->
        to_native(type, Enum.at(value, index))

      {[column, "", type], _index} when is_map(value) ->
        to_native(type, Map.get(value, column))

      {[_column, "", type], index} when is_list(value) ->
        to_native(type, Enum.at(value, index))
    end)
  end

  defp to_native(<<"Map(", map_types::binary>>, value) do
    map_types = String.replace_suffix(map_types, ")", "")
    [key_type, value_type] = String.split(map_types, ", ", parts: 2)

    value
    |> Enum.reduce(%{}, fn {key, value}, acc -> Map.put(acc, to_native(key_type, key), to_native(value_type, value)) end)
  end

  defp to_native("Float" <> _, value) when is_integer(value) do
    1.0 * value
  end

  defp to_native("Int64", value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_native("Date", value) do
    {:ok, date} = to_date(value)
    date
  end

  defp to_native("DateTime", value) do
    [date, time] = String.split(value, " ")

    with {:ok, date} <- to_date(date),
         {:ok, time} <- to_time(time),
         {:ok, naive} <- NaiveDateTime.new(date, time) do
      naive
    end
  end

  defp to_native("UInt" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_native("Int" <> _, value) when is_bitstring(value) do
    String.to_integer(value)
  end

  defp to_native("Decimal" <> _, value) when @use_decimal and (is_bitstring(value) or is_integer(value)) do
    Decimal.new(value)
  end

  defp to_native("Decimal" <> _, value) when is_bitstring(value) do
    String.to_float(value)
  end

  defp to_native(<<"SimpleAggregateFunction(", type::binary>>, value) do
    do_aggregate_function(type, value)
  end

  defp to_native(<<"AggregateFunction(", type::binary>>, value) do
    do_aggregate_function(type, value)
  end

  defp to_native(_, value) do
    value
  end

  defp do_aggregate_function(type, value) do
    type = String.replace_suffix(type, ")", "")
    [_, inner_type] = String.split(type, ",", parts: 2, trim: true)
    inner_type = String.trim(inner_type)

    to_native(inner_type, value)
  end

  defp to_date(date_string) do
    date_string
    |> String.split("-")
    |> Enum.map(&String.to_integer/1)
    |> case do
      [0, 0, 0] -> Date.new(1970, 1, 1)
      [year, month, day] -> Date.new(year, month, day)
    end
  end

  defp to_time(time_string) do
    [h, m, s] =
      time_string
      |> String.split(":")
      |> Enum.map(&String.to_integer/1)

    Time.new(h, m, s)
  end
end
