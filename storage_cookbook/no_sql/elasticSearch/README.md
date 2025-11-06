# Elasticsearch - (Search engine)

<img src="images/elastic_feat.png" />

- É um banco de dados NoSQL que se enquadra como  **search engine**.

- Para o que é utilizado?<br/>
Buscas Geoespaciais, Logs Transacionais, Classificação e Ranking de resultados de pesquisa, sempre focando a performance em buscas em uma extensa massa de dados.
- Acesso via API RESTfull (get - post - put - delete)
- É um [SGBD](https://db-engines.com/en/blog_post/70)

- inverted-index


### Comparativos

- Orientado à documentos (json), assim como o MongoDB ( comparativo: https://medium.com/data-hackers/comparando-elasticsearch-vs-mongodb-4b5932c613d9)

- index **=** banco de dados

- tipo de documento (tipo de entidade, ex) filme) = tipo de tabela
<br/>

- Tutorial install:<br/>
https://www.linode.com/docs/databases/elasticsearch/a-guide-to-elasticsearch-plugins/

### Enable and start the elasticsearch service
```
sudo systemctl enable elasticsearch
sudo systemctl start elasticsearch
```
- Test<br/>
`curl localhost:9200`
<br/>

<img src="images/run_elastic.png" />


- Status<br/>
`systemctl status elasticsearch`
<br/>

<img src="images/status_server.png" />


## Elastic Stack
<img src="images/icon-elastic-stack-bb.svg" />



#### Kibana Dashboard
- É o dashboard
- Trás um console parecido com o Postman


#### Enable and start the Kibana service
```
sudo systemctl enable Kibana
sudo systemctl start Kibana
```
- Test<br/>
`curl localhost:5601
`


### Beats: 
- Envio de dados do tipo log, metrics, network, uptime

### Logstash: 
- ETL, consulta SQL


## CRUD
E sintaxe padrão é:<br/>
`METODO_HTTP /NOME_INDICE/TYPE/`

Vou inserir dados de restaurantes de San Franscisco.

#### POST
TERMINAL
```
curl -XPOST "http://localhost:9200/inspections/report" -H 'Content-Type: application/json' -d'
{
  "business_address": "660 Sacramento St",
  "business_city": "San Francisco",
  "business_id": "2228",
  "business_latitude": "37.793698",
  "business_location": {
    "type": "Point",
    "coordinates": [
      -122.403984,
      37.793698
    ]
  },
  "business_longitude": "-122.403984",
  "business_name": "Tokyo Express",
  "business_postal_code": "94111",
  "business_state": "CA",
  "inspection_date": "2016-02-04T00:00:00.000",
  "inspection_id": "2228_20160204",
  "inspection_type": "Routine",
  "inspection_score":96,
  "risk_category": "Low Risk",
  "violation_description": "Unclean nonfood contact surfaces",
  "violation_id": "2228_20160204_103142"
}'
```
Result<br/>
```
{
  "_index" : "inspections",
  "_type" : "report",
  "_id" : "ab5khmcB0hFL3As9i3-O",
  "_version" : 1,
  "result" : "created",
  "_shards" : {
    "total" : 2,
    "successful" : 1,
    "failed" : 0
  },
  "_seq_no" : 0,
  "_primary_term" : 2
}
```

**Kibana**<br/>
Não é necessário usar curl, basta inserir o método na API.

<img src="images/elastic_post.png" />

#### POST in lot
Para inserir vários documentos json é utilizado o `_bulk`.<br/>
Deve ser seguido a convesão de inserção:<br/>
- metadado
- documento
- metadado
- documento<br/>
- ...

Ex)<br/>
TERMINAL
```
POST /inspections/report/_bulk
{ "index": { "_id": 1 }}
{"business_address":"315 California St","business_city":"San Francisco","business_id":"24936","business_latitude":"37.793199","business_location":{"type":"Point","coordinates":[-122.400152,37.793199]},"business_longitude":"-122.400152","business_name":"San Francisco Soup Company","business_postal_code":"94104","business_state":"CA","inspection_date":"2016-06-09T00:00:00.000","inspection_id":"24936_20160609","inspection_score":77,"inspection_type":"Routine - Unscheduled","risk_category":"Low Risk","violation_description":"Improper food labeling or menu misrepresentation","violation_id":"24936_20160609_103141"}

{ "index": { "_id": 2 }}
{"business_address":"10 Mason St","business_city":"San Francisco","business_id":"60354","business_latitude":"37.783527","business_location":{"type":"Point","coordinates":[-122.409061,37.783527]},"business_longitude":"-122.409061","business_name":"Soup Unlimited","business_postal_code":"94102","business_state":"CA","inspection_date":"2016-11-23T00:00:00.000","inspection_id":"60354_20161123","inspection_type":"Routine", "inspection_score": 95}

{ "index": { "_id": 3 }}
{"business_address":"2872 24th St","business_city":"San Francisco","business_id":"1797","business_latitude":"37.752807","business_location":{"type":"Point","coordinates":[-122.409752,37.752807]},"business_longitude":"-122.409752","business_name":"TIO CHILOS GRILL","business_postal_code":"94110","business_state":"CA","inspection_date":"2016-07-05T00:00:00.000","inspection_id":"1797_20160705","inspection_score":90,"inspection_type":"Routine - Unscheduled","risk_category":"Low Risk","violation_description":"Unclean nonfood contact surfaces","violation_id":"1797_20160705_103142"}

{ "index": { "_id": 4 }}
{"business_address":"1661 Tennessee St Suite 3B","business_city":"San Francisco Whard Restaurant","business_id":"66198","business_latitude":"37.75072","business_location":{"type":"Point","coordinates":[-122.388478,37.75072]},"business_longitude":"-122.388478","business_name":"San Francisco Restaurant","business_postal_code":"94107","business_state":"CA","inspection_date":"2016-05-27T00:00:00.000","inspection_id":"66198_20160527","inspection_type":"Routine","inspection_score":56 }

{ "index": { "_id": 5 }}
{"business_address":"2162 24th Ave","business_city":"San Francisco","business_id":"5794","business_latitude":"37.747228","business_location":{"type":"Point","coordinates":[-122.481299,37.747228]},"business_longitude":"-122.481299","business_name":"Soup House","business_phone_number":"+14155752700","business_postal_code":"94116","business_state":"CA","inspection_date":"2016-09-07T00:00:00.000","inspection_id":"5794_20160907","inspection_score":96,"inspection_type":"Routine - Unscheduled","risk_category":"Low Risk","violation_description":"Unapproved or unmaintained equipment or utensils","violation_id":"5794_20160907_103144"}

{ "index": { "_id": 6 }}
{"business_address":"2162 24th Ave","business_city":"San Francisco","business_id":"5794","business_latitude":"37.747228","business_location":{"type":"Point","coordinates":[-122.481299,37.747228]},"business_longitude":"-122.481299","business_name":"Soup-or-Salad","business_phone_number":"+14155752700","business_postal_code":"94116","business_state":"CA","inspection_date":"2016-09-07T00:00:00.000","inspection_id":"5794_20160907","inspection_score":96,"inspection_type":"Routine - Unscheduled","risk_category":"Low Risk","violation_description":"Unapproved or unmaintained equipment or utensils","violation_id":"5794_20160907_103144"}
```

Result<br/>
Para cada documento inserido há uma resposta.<br/>
```
{
  "took" : 2270,
  "errors" : false,
  "items" : [
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "1",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 0,
        "_primary_term" : 1,
        "status" : 201
      }
    },
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "2",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 0,
        "_primary_term" : 1,
        "status" : 201
      }
    },
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "3",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 0,
        "_primary_term" : 1,
        "status" : 201
      }
    },
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "4",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 1,
        "_primary_term" : 1,
        "status" : 201
      }
    },
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "5",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 0,
        "_primary_term" : 1,
        "status" : 201
      }
    },
    {
      "index" : {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "6",
        "_version" : 1,
        "result" : "created",
        "_shards" : {
          "total" : 2,
          "successful" : 1,
          "failed" : 0
        },
        "_seq_no" : 2,
        "_primary_term" : 1,
        "status" : 201
      }
    }
  ]
}
```

#### GET
Toda busca gera um ID.<br/>
Ex: 
`GET /NOME_INDICE/TYPE/_search`

`curl -XGET "http://localhost:9200/inspections/report/_search"`

**Result**
```
{
  "took" : 0,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 1,
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "rHtChmcBelYutwH1V1V3",
        "_score" : 1.0,
        "_source" : {
          "business_address" : "660 Sacramento St",
          "business_city" : "San Francisco",
          "business_id" : "2228",
          "business_latitude" : "37.793698",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.403984,
              37.793698
            ]
          }
        }
      }
    ]
  }
}
```
#### DELETE
Apagar o índice (banco de dados).<br/>
`curl -XDELETE "http://localhost:9200/inspections"`

**Result**<br/>
`{"acknowledged":true}`


#### GET (SELECT * FROM report WHERE ____ )
No elastic, as buscas tem um score para cada item retornado.

```
GET inspections/report/_search
{
  "query": {
    "match": {

    }
  }
}
```
Buscar os restaurantes que vendem `soup`<br/>

```
GET inspections/report/_search
{
  "query": {
    "match": {
      "business_name": "soup"
    }
  }
}
```
**Result**
```
{
  "took" : 0,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 4,
    "max_score" : 0.52354836,
    "hits" : [
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "2",
        "_score" : 0.52354836,
        "_source" : {
          "business_address" : "10 Mason St",
          "business_city" : "San Francisco",
          "business_id" : "60354",
          "business_latitude" : "37.783527",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.409061,
              37.783527
            ]
          },
          "business_longitude" : "-122.409061",
          "business_name" : "Soup Unlimited",
          "business_postal_code" : "94102",
          "business_state" : "CA",
          "inspection_date" : "2016-11-23T00:00:00.000",
          "inspection_id" : "60354_20161123",
          "inspection_type" : "Routine",
          "inspection_score" : 95
        }
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "6",
        "_score" : 0.4471386,
        "_source" : {
          "business_address" : "2162 24th Ave",
          "business_city" : "San Francisco",
          "business_id" : "5794",
          "business_latitude" : "37.747228",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.481299,
              37.747228
            ]
          },
          "business_longitude" : "-122.481299",
          "business_name" : "Soup-or-Salad",
          "business_phone_number" : "+14155752700",
          "business_postal_code" : "94116",
          "business_state" : "CA",
          "inspection_date" : "2016-09-07T00:00:00.000",
          "inspection_id" : "5794_20160907",
          "inspection_score" : 96,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Unapproved or unmaintained equipment or utensils",
          "violation_id" : "5794_20160907_103144"
        }
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "5",
        "_score" : 0.2876821,
        "_source" : {
          "business_address" : "2162 24th Ave",
          "business_city" : "San Francisco",
          "business_id" : "5794",
          "business_latitude" : "37.747228",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.481299,
              37.747228
            ]
          },
          "business_longitude" : "-122.481299",
          "business_name" : "Soup House",
          "business_phone_number" : "+14155752700",
          "business_postal_code" : "94116",
          "business_state" : "CA",
          "inspection_date" : "2016-09-07T00:00:00.000",
          "inspection_id" : "5794_20160907",
          "inspection_score" : 96,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Unapproved or unmaintained equipment or utensils",
          "violation_id" : "5794_20160907_103144"
        }
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "1",
        "_score" : 0.2876821,
        "_source" : {
          "business_address" : "315 California St",
          "business_city" : "San Francisco",
          "business_id" : "24936",
          "business_latitude" : "37.793199",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.400152,
              37.793199
            ]
          },
          "business_longitude" : "-122.400152",
          "business_name" : "San Francisco Soup Company",
          "business_postal_code" : "94104",
          "business_state" : "CA",
          "inspection_date" : "2016-06-09T00:00:00.000",
          "inspection_id" : "24936_20160609",
          "inspection_score" : 77,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Improper food labeling or menu misrepresentation",
          "violation_id" : "24936_20160609_103141"
        }
      }
    ]
  }
}
```

#### GET (SELECT * FROM report WHERE ____ ORDER BY ____ )
Retornar os restaurantes com inspection_score > 80 em ordem descrescente.

```
GET /inspections/report/_search
{
  "query": {
      "range": {
        "inspection_score": {
          "gte": 80
        }
      }
  },
  "sort": [
    { "inspection_score" : "desc" }
  ]
}
```

**Result**

```
{
  "took" : 7,
  "timed_out" : false,
  "_shards" : {
    "total" : 5,
    "successful" : 5,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : 4,
    "max_score" : null,
    "hits" : [
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "5",
        "_score" : null,
        "_source" : {
          "business_address" : "2162 24th Ave",
          "business_city" : "San Francisco",
          "business_id" : "5794",
          "business_latitude" : "37.747228",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.481299,
              37.747228
            ]
          },
          "business_longitude" : "-122.481299",
          "business_name" : "Soup House",
          "business_phone_number" : "+14155752700",
          "business_postal_code" : "94116",
          "business_state" : "CA",
          "inspection_date" : "2016-09-07T00:00:00.000",
          "inspection_id" : "5794_20160907",
          "inspection_score" : 96,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Unapproved or unmaintained equipment or utensils",
          "violation_id" : "5794_20160907_103144"
        },
        "sort" : [
          96
        ]
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "6",
        "_score" : null,
        "_source" : {
          "business_address" : "2162 24th Ave",
          "business_city" : "San Francisco",
          "business_id" : "5794",
          "business_latitude" : "37.747228",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.481299,
              37.747228
            ]
          },
          "business_longitude" : "-122.481299",
          "business_name" : "Soup-or-Salad",
          "business_phone_number" : "+14155752700",
          "business_postal_code" : "94116",
          "business_state" : "CA",
          "inspection_date" : "2016-09-07T00:00:00.000",
          "inspection_id" : "5794_20160907",
          "inspection_score" : 96,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Unapproved or unmaintained equipment or utensils",
          "violation_id" : "5794_20160907_103144"
        },
        "sort" : [
          96
        ]
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "2",
        "_score" : null,
        "_source" : {
          "business_address" : "10 Mason St",
          "business_city" : "San Francisco",
          "business_id" : "60354",
          "business_latitude" : "37.783527",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.409061,
              37.783527
            ]
          },
          "business_longitude" : "-122.409061",
          "business_name" : "Soup Unlimited",
          "business_postal_code" : "94102",
          "business_state" : "CA",
          "inspection_date" : "2016-11-23T00:00:00.000",
          "inspection_id" : "60354_20161123",
          "inspection_type" : "Routine",
          "inspection_score" : 95
        },
        "sort" : [
          95
        ]
      },
      {
        "_index" : "inspections",
        "_type" : "report",
        "_id" : "3",
        "_score" : null,
        "_source" : {
          "business_address" : "2872 24th St",
          "business_city" : "San Francisco",
          "business_id" : "1797",
          "business_latitude" : "37.752807",
          "business_location" : {
            "type" : "Point",
            "coordinates" : [
              -122.409752,
              37.752807
            ]
          },
          "business_longitude" : "-122.409752",
          "business_name" : "TIO CHILOS GRILL",
          "business_postal_code" : "94110",
          "business_state" : "CA",
          "inspection_date" : "2016-07-05T00:00:00.000",
          "inspection_id" : "1797_20160705",
          "inspection_score" : 90,
          "inspection_type" : "Routine - Unscheduled",
          "risk_category" : "Low Risk",
          "violation_description" : "Unclean nonfood contact surfaces",
          "violation_id" : "1797_20160705_103142"
        },
        "sort" : [
          90
        ]
      }
    ]
  }
}
```
