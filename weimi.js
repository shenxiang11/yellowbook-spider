const { Kafka } = require('kafkajs')
const axios = require('axios')

const kafka = new Kafka({
  brokers: [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
    "localhost:9095",
  ],
})

const producer = kafka.producer()

!(async () => {
  await producer.connect()
  let page = 1

  while (true) {
    const res = await fetchByPage(page)
    if (res.code !== 200) {
      break
    }

    console.log(page);

    res.data.data.map(async item => {
      let title = item.TopShortTitle
      let content = item.Title
      let imageList = item.ImageList
      if (imageList.length) {
        imageList = imageList.map(image => {
          return image.FileUrl;
        })
      }
      await producer.send({
        topic: 'weimi_luyao',
        messages: [
          {
            value: JSON.stringify({
              title,
              content,
              imageList,
            }),
          },
        ]
      })
    })
    page += 1;
  }

  await producer.disconnect()
})()

function fetchByPage(page) {
  let data = `token=6860610d64c24401adaf48d19ccc664e&data=%7B%22ViewSourceType%22%3A4%2C%22Key%22%3A%22%22%2C%22PageIndex%22%3A${page}%2C%22PageSize%22%3A10%2C%22CommunityId%22%3A%22173240%22%2C%22ViewType%22%3A0%2C%22SortType%22%3A2%2C%22SourceFrom%22%3A1%2C%22TagId%22%3A0%7D`;

  let config = {
    method: 'post',
    maxBodyLength: Infinity,
    url: 'http://topic00.weme.link/CommunityIndex/GetCommunityPostList?test=mincro',
    headers: {
      'Host': 'topic00.weme.link',
      'Accept': 'application/json, text/javascript, */*; q=0.01',
      'wm': '1695883840591',
      'ostype': '1',
      'Accept-Language': 'zh-CN,zh-Hans;q=0.9',
      'Origin': 'http://m.weme.link',
      'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) MainCircleChidController',
      'Connection': 'keep-alive',
      'Referer': 'http://m.weme.link/',
      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'Authorization': 'Bearer eyJhbGciOiJSUzUxMiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJ5ZWxsb2Jvb2siLCJzdWIiOiIxIiwiZXhwIjoxNjk1ODkzNDQyLCJpYXQiOjE2OTU4OTI4NDJ9.ouYBhFGJ3Nm42dm_3cjvK8QirZfvExN4jklXilzbsLOlZeKIzlYXsVbGg7TQvz091l4mnrwFWHD4gr2x4sM85GdVJIoggj0G6xhtQsvTKB5-Jw2Yj07B1ucj4r-HM-M5pc6mJ_KWIupjwfO91dx0I-jnw2PijYj2MchlUqBy5n0'
    },
    data : data
  };

  return axios.request(config)
    .then((response) => {
      return response.data;
    })
    .catch((error) => {
      console.log(error);
    });

}