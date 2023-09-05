const axios = require('axios');
const fs = require('fs');


const main = async () => {

  const search_query = {
    query: {
    bool: {
      filter: {
        bool: {
          must: [
            {term: { 'location.countryCode': "IT"}},
            {term: { status: "active"}}
          ],
          must_not: [
            {exists: { field: 'location.isoSubDivisionCode'}}
          ]
        }
      }
    }
    },
    sort: [
      {_id: "desc"}    
    ],
    track_total_hits: true,
    stored_fields: ["_id"],
    size: 1000
  }

  const elasticSearchEndpoint = "https://vpc-bg-prod-elasticsearch-z7ibuk6lcrixgi7k465ngudawe.us-east-1.es.amazonaws.com/boat/_search";
  const listingMonitorEndpoint = "http://api-node-listing-monitor.prod.bgrp.io/queue-listing?db=imt&alias=boat&category=boat&id="

  const response = await axios.post(elasticSearchEndpoint, search_query);

  // console.log(response);

  let ids = response.data.hits.hits.map(hit => hit._id);
  let total_hits = response['data']['hits']['total']['value']
  let lastId = ids[ids.length - 1];
  while (ids.length < total_hits){
    try {
        search_query.search_after = [lastId];
        const response = await axios.post(elasticSearchEndpoint, search_query);
        const newIds = response.data.hits.hits.map(hit => hit._id);
        ids.push(...newIds);
        lastId = ids[ids.length - 1];
        console.log(ids.length);
    }
    catch (error) {
      console.log(error);
      break;
    }
  }
  console.log('total_hits -', total_hits);
  console.log('ids.length -', ids.length);
  
  for (i = 0; i < ids.length; i++) {
    try {
      await axios.post(listingMonitorEndpoint + ids[i]);
      if (i % 100 === 0) console.log('listings indexed: ', i);
    }
    catch (error) {
      console.log(error);
    }
  }

  let file = fs.createWriteStream('ids.txt');
  file.on('error', function(err) { console.log(err)});
  file.write(ids.join(', ') + '\n');
  file.end();

}

main();