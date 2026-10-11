defmodule SimpleGrpc.ErrorDetails do
  @moduledoc """
  Builders and helpers for standard Google gRPC rich error details (`google.rpc.*`).

  Provides constructors for:
  - `ErrorInfo`
  - `BadRequest`
  - `RetryInfo`
  - `DebugInfo`
  - `QuotaFailure`
  - `PreconditionFailure`
  - `ResourceInfo`
  - `Help`
  - `LocalizedMessage`
  - `RequestInfo`

  Supports automatic serialization and packing into `google.protobuf.Any`.
  """

  alias SimpleGrpc.WKT
  alias SimpleGrpc.Protobuf
  alias SimpleGrpc.Schema.WellKnownTypes

  @type_url_prefix "type.googleapis.com/"

  # ===========================================================================
  # Error Detail Builders
  # ===========================================================================

  @doc """
  Constructs a `google.rpc.ErrorInfo` payload.

  Describes the cause of the error with structured metadata.
  """
  def error_info(reason, domain, metadata \\ %{}) do
    meta_map =
      Enum.into(metadata, %{}, fn {k, v} ->
        {to_string(k), to_string(v)}
      end)

    %{
      reason: to_string(reason),
      domain: to_string(domain),
      metadata: meta_map
    }
  end

  @doc """
  Constructs a `google.rpc.BadRequest` payload with field violations.

  `field_violations` can be a list of maps, keywords, or `{field, description}` tuples.
  """
  def bad_request(field_violations) when is_list(field_violations) do
    violations =
      Enum.map(field_violations, fn
        {field, desc} ->
          %{field: to_string(field), description: to_string(desc)}

        %{field: f, description: d} ->
          %{field: to_string(f), description: to_string(d)}

        %{"field" => f, "description" => d} ->
          %{field: to_string(f), description: to_string(d)}

        kw when is_list(kw) ->
          %{
            field: to_string(Keyword.get(kw, :field, "")),
            description: to_string(Keyword.get(kw, :description, ""))
          }
      end)

    %{field_violations: violations}
  end

  @doc """
  Constructs a `google.rpc.RetryInfo` payload.

  `retry_delay` can be integer seconds, float seconds, integer milliseconds (if specified with unit or duration struct),
  or a `google.protobuf.Duration` map (`%{seconds: s, nanos: n}`).
  """
  def retry_info(%{seconds: _, nanos: _} = duration) do
    %{retry_delay: duration}
  end

  def retry_info(%{"seconds" => _, "nanos" => _} = duration) do
    %{retry_delay: duration}
  end

  def retry_info(seconds) when is_integer(seconds) or is_float(seconds) do
    %{retry_delay: WKT.Duration.from_seconds(seconds)}
  end

  @doc """
  Constructs a `google.rpc.DebugInfo` payload.

  Contains stack trace entries and additional debug details.
  """
  def debug_info(stack_entries, detail \\ "") do
    entries =
      stack_entries
      |> List.wrap()
      |> Enum.map(&to_string/1)

    %{
      stack_entries: entries,
      detail: to_string(detail || "")
    }
  end

  @doc """
  Constructs a `google.rpc.QuotaFailure` payload with quota violations.

  `violations` can be a list of maps or `{subject, description}` tuples.
  """
  def quota_failure(violations) when is_list(violations) do
    v_list =
      Enum.map(violations, fn
        {subject, desc} ->
          %{subject: to_string(subject), description: to_string(desc)}

        %{subject: s, description: d} ->
          %{subject: to_string(s), description: to_string(d)}

        %{"subject" => s, "description" => d} ->
          %{subject: to_string(s), description: to_string(d)}

        kw when is_list(kw) ->
          %{
            subject: to_string(Keyword.get(kw, :subject, "")),
            description: to_string(Keyword.get(kw, :description, ""))
          }
      end)

    %{violations: v_list}
  end

  @doc """
  Constructs a `google.rpc.PreconditionFailure` payload with precondition violations.

  `violations` can be a list of maps or `{type, subject, description}` tuples.
  """
  def precondition_failure(violations) when is_list(violations) do
    v_list =
      Enum.map(violations, fn
        {type, subject, desc} ->
          %{type: to_string(type), subject: to_string(subject), description: to_string(desc)}

        %{type: t, subject: s, description: d} ->
          %{type: to_string(t), subject: to_string(s), description: to_string(d)}

        %{"type" => t, "subject" => s, "description" => d} ->
          %{type: to_string(t), subject: to_string(s), description: to_string(d)}

        kw when is_list(kw) ->
          %{
            type: to_string(Keyword.get(kw, :type, "")),
            subject: to_string(Keyword.get(kw, :subject, "")),
            description: to_string(Keyword.get(kw, :description, ""))
          }
      end)

    %{violations: v_list}
  end

  @doc """
  Constructs a `google.rpc.ResourceInfo` payload.

  Describes a resource being accessed or modified.
  """
  def resource_info(resource_type, resource_name, owner \\ "", description \\ "") do
    %{
      resource_type: to_string(resource_type),
      resource_name: to_string(resource_name),
      owner: to_string(owner || ""),
      description: to_string(description || "")
    }
  end

  @doc """
  Constructs a `google.rpc.Help` payload with helpful documentation links.

  `links` can be a list of maps or `{description, url}` tuples.
  """
  def help(links) when is_list(links) do
    link_list =
      Enum.map(links, fn
        {desc, url} ->
          %{description: to_string(desc), url: to_string(url)}

        %{description: d, url: u} ->
          %{description: to_string(d), url: to_string(u)}

        %{"description" => d, "url" => u} ->
          %{description: to_string(d), url: to_string(u)}

        kw when is_list(kw) ->
          %{
            description: to_string(Keyword.get(kw, :description, "")),
            url: to_string(Keyword.get(kw, :url, ""))
          }
      end)

    %{links: link_list}
  end

  @doc """
  Constructs a `google.rpc.LocalizedMessage` payload.

  Provides user-facing error message localized for a specific locale (e.g. "en-US", "fr-CA").
  """
  def localized_message(locale, message) do
    %{
      locale: to_string(locale),
      message: to_string(message)
    }
  end

  @doc """
  Constructs a `google.rpc.RequestInfo` payload with request metadata.
  """
  def request_info(request_id, serving_data \\ "") do
    %{
      request_id: to_string(request_id),
      serving_data: to_string(serving_data || "")
    }
  end

  # ===========================================================================
  # Any Packing & Unpacking
  # ===========================================================================

  @doc """
  Packs a detail map into a `google.protobuf.Any` map (`%{type_url: url, value: bytes}`).
  If the payload is already an Any map (`%{type_url: _, value: _}`), returns it as is.
  """
  def pack(detail, type_name \\ nil)

  def pack(%{type_url: _, value: _} = already_packed, _), do: already_packed
  def pack(%{"type_url" => _, "value" => _} = already_packed, _), do: already_packed

  def pack(detail, type_name) when is_map(detail) do
    inferred_type = type_name || infer_type_name(detail)

    if inferred_type do
      schema_ctx = WellKnownTypes.unified_schema()
      full_type_url = @type_url_prefix <> inferred_type
      encoded_bytes = Protobuf.encode!(inferred_type, detail, schema_ctx)
      %{type_url: full_type_url, value: encoded_bytes}
    else
      # If unknown type, treat as google.protobuf.Struct or pack directly
      WKT.pack_any(detail, "google.protobuf.Struct")
    end
  end

  @doc """
  Unpacks a `google.protobuf.Any` map into a native Elixir error details map.
  If already unpacked, returns the map directly.
  """
  def unpack(any_map, schema \\ nil)

  def unpack(%{type_url: type_url, value: value}, schema)
      when is_binary(type_url) and is_binary(value) do
    schema_ctx = schema || WellKnownTypes.unified_schema()
    type_name = extract_type_name(type_url)

    case Protobuf.decode(type_name, value, schema_ctx) do
      {:ok, decoded} ->
        # Attach __type_url__ or :_type metadata for clear caller inspection
        Map.put(decoded, :__type__, type_name)

      _ ->
        %{type_url: type_url, value: value}
    end
  end

  def unpack(%{"type_url" => type_url, "value" => value}, schema) do
    unpack(%{type_url: type_url, value: value}, schema)
  end

  def unpack(other, _schema), do: other

  @doc """
  Unpacks all detail items in a list.
  """
  def unpack_all(details, schema \\ nil)

  def unpack_all(details, schema) when is_list(details) do
    Enum.map(details, &unpack(&1, schema))
  end

  def unpack_all(other, _schema), do: List.wrap(other)

  # ---------------------------------------------------------------------------
  # Internal Type Inference & Helpers
  # ---------------------------------------------------------------------------

  defp infer_type_name(map) when is_map(map) do
    cond do
      Map.has_key?(map, :reason) and Map.has_key?(map, :domain) ->
        "google.rpc.ErrorInfo"

      Map.has_key?(map, :field_violations) ->
        "google.rpc.BadRequest"

      Map.has_key?(map, :retry_delay) ->
        "google.rpc.RetryInfo"

      Map.has_key?(map, :stack_entries) ->
        "google.rpc.DebugInfo"

      Map.has_key?(map, :violations) and
          match?([%{type: _, subject: _} | _], Map.get(map, :violations)) ->
        "google.rpc.PreconditionFailure"

      Map.has_key?(map, :violations) ->
        "google.rpc.QuotaFailure"

      Map.has_key?(map, :resource_type) and Map.has_key?(map, :resource_name) ->
        "google.rpc.ResourceInfo"

      Map.has_key?(map, :links) ->
        "google.rpc.Help"

      Map.has_key?(map, :locale) and Map.has_key?(map, :message) ->
        "google.rpc.LocalizedMessage"

      Map.has_key?(map, :request_id) ->
        "google.rpc.RequestInfo"

      true ->
        nil
    end
  end

  defp extract_type_name(type_url) when is_binary(type_url) do
    case String.split(type_url, "/", parts: 2) do
      [_prefix, name] -> name
      [name] -> name
    end
  end
end
