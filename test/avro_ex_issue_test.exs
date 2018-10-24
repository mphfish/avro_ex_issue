defmodule AvroExIssueTest do
  use ExUnit.Case

  test "1 does not decode" do
    {:ok, schema} = AvroEx.parse_schema(~S({
      "type" : "record",
      "name" : "FirstLevel",
      "namespace" : "com.asurint.data.models",
      "fields" : [ {
        "name" : "secondLevel",
        "type" : [ "null", {
          "type" : "record",
          "name" : "SecondLevel",
          "fields" : [ {
            "name" : "thirdLevel",
            "type" : [ "null", {
              "type" : "record",
              "name" : "ThirdLevel",
              "fields" : [ {
                "name" : "fourthLevels",
                "type" : {
                  "type" : "array",
                  "items" : {
                    "type" : "record",
                    "name" : "FourthLevel",
                    "fields" : [ {
                      "name" : "integer",
                      "type" : [ "null", "sring" ]
                    } ]
                  }
                }
              } ]
            } ]
          } ]
        } ]
      }, {
        "name" : "siblings",
        "type" : {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "Sibling",
            "fields" : [ {
              "name" : "integer",
              "type" : [ "null", "string" ]
            } ]
          }
        }
      } ]
    }))

    message_body = %{
      "secondLevel" => %{
        "thirdLevel" => %{
          "fourthLevels" => []
        }
      },
      "siblings" => [
        %{
          "integer" => "6"
        }
      ]
    }

    {:ok, encoded} = AvroEx.encode(schema, message_body)

    {:ok, message_body} = AvroEx.decode(schema, encoded)
  end

  test "2 does not decode" do
    {:ok, schema} = AvroEx.parse_schema(~S({
      "type" : "record",
      "name" : "FirstLevel",
      "namespace" : "com.asurint.data.models",
      "fields" : [ {
        "name" : "secondLevel",
        "type" : [ "null", {
          "type" : "record",
          "name" : "SecondLevel",
          "fields" : [ {
            "name" : "thirdLevel",
            "type" : [ "null", {
              "type" : "record",
              "name" : "ThirdLevel",
              "fields" : [ {
                "name" : "fourthLevels",
                "type" : {
                  "type" : "array",
                  "items" : {
                    "type" : "record",
                    "name" : "FourthLevel",
                    "fields" : [ {
                      "name" : "integer",
                      "type" : [ "null", "int" ]
                    } ]
                  }
                }
              } ]
            } ]
          } ]
        } ]
      }, {
        "name" : "siblings",
        "type" : {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "Sibling",
            "fields" : [ {
              "name" : "integer",
              "type" : [ "null", "int" ]
            } ]
          }
        }
      } ]
    }))

    message_body = %{
      "secondLevel" => %{
        "thirdLevel" => %{
          "fourthLevels" => []
        }
      },
      "siblings" => [
        %{
          "integer" => 1
        }
      ]
    }

    {:ok, encoded} = AvroEx.encode(schema, message_body)

    {:ok, message_body} = AvroEx.decode(schema, encoded)
  end

  test "encodes and decodes a sufficiently complex avro model" do
    {:ok, schema} = AvroEx.parse_schema(~S({
      "type" : "record",
      "name" : "FirstLevel",
      "namespace" : "com.asurint.data.models",
      "fields" : [ {
        "name" : "secondLevel",
        "type" : [ "null", {
          "type" : "record",
          "name" : "SecondLevel",
          "fields" : [ {
            "name" : "thirdLevel",
            "type" : [ "null", {
              "type" : "record",
              "name" : "ThirdLevel",
              "fields" : [ {
                "name" : "fourthLevels",
                "type" : {
                  "type" : "array",
                  "items" : {
                    "type" : "record",
                    "name" : "FourthLevel",
                    "fields" : [ {
                      "name" : "integer",
                      "type" : [ "null", "int" ]
                    } ]
                  }
                }
              } ]
            } ]
          } ]
        } ]
      }, {
        "name" : "siblings",
        "type" : {
          "type" : "array",
          "items" : {
            "type" : "record",
            "name" : "Sibling",
            "fields" : [ {
              "name" : "integer",
              "type" : [ "null", "int" ]
            } ]
          }
        }
      } ]
    }))

    message_body = %{
      "secondLevel" => %{
        "thirdLevel" => %{
          "fourthLevels" => [
            %{
              "integer" => 1
            }
          ]
        }
      },
      "siblings" => [
        %{
          "integer" => 1
        }
      ]
    }

    {:ok, encoded} = AvroEx.encode(schema, message_body)

    {:ok, message_body} = AvroEx.decode(schema, encoded)
  end
end
