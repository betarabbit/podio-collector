// load env variables
require('dotenv').config()

const azure = require('azure-storage')
const Podio = require('podio-js').api
const config = require('./config')

const api = new Podio({
  authType: 'password',
  clientId: process.env.ID,
  clientSecret: process.env.SECRET
})

const retryOperation = new azure.ExponentialRetryPolicyFilter()
const tableService = azure.createTableService().withFilter(retryOperation)

function collectCoursePool () {
  return getPodioAppItems(config.course_pool_app_id)
    .then(data => data.map(
      item => item.fields.reduce(
        (acc, val) => Object.assign(
          acc,
          { id: item.item_id },
          { [val.label]: getValue(val) }
        ), {}))
    )
}

function collectCourseOffer () {
  return Promise.all(config.offer.map(
    offer => getPodioAppItems(offer.app_id)
      .then(data => data.map(
        item => item.fields.reduce(
          (acc, val) => Object.assign(
            acc,
            {
              id: item.item_id,
              Year: offer.year
            },
            { [val.label]: getValue(val) }
          ), {})
      ))
  ))
}

function collectTrainingPlan () {
  return Promise.all(config.team.map(
    team => getPodioAppItems(team.app_id)
      .then(data => data.map(
        item => item.fields
          .filter(field => field.label === 'Attendee Name')
          .map(
            field => item.fields
              .filter(field => field.label !== 'Attendee Name')
              .reduce(
                (acc, val) => Object.assign(
                  acc,
                  {
                    id: item.item_id,
                    Year: team.year,
                    Team: team.name,
                    'Attendee Name': getValue(field)
                  },
                  { [val.label]: getValue(val) }
                ), {})
              )
          )
      ))
  )
}

function collectData () {
  return Promise.all([collectCoursePool(), collectCourseOffer(), collectTrainingPlan()])
    .then(values => {
      const [coursePool, courseOffer, trainingPlan] = values
      return {
        coursePool,
        courseOffer: flatten(courseOffer),
        trainingPlan: flatten(trainingPlan)
      }
    })
}

function deleteAzureTable (service, tableName) {
  return new Promise((resolve, reject) =>
    service.deleteTableIfExists(tableName, (err, response) => {
      if (err) {
        reject(err)
      } else {
        resolve(response)
      }
    })
  )
}

function createAzureTable (service, tableName) {
  return new Promise((resolve, reject) =>
    service.createTableIfNotExists(tableName, (err, result, response) => {
      if (err) {
        reject(err)
      } else {
        resolve({ result, response })
      }
    })
  )
}

function uploadToAzure (data) {
  data.map()

  const batch = new azure.TableBatch()
}

function generateEntities (data, partitionKey, rowKey = 'id') {
  const entGen = azure.TableUtilities.entityGenerator

  return data.map(d => {
    const entity = Object.keys(d).reduce((acc, key) => {
      if (/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/.test(d[key])) {
        // suppose this app only used in China office
        acc[key] = entGen.DateTime(new Date(`${d[key]} GMT+0800`))
      } else {
        acc[key] = entGen.String(d[key])
      }

      return acc
    }, {})

    return Object.assign(entity, {
      PartitionKey: entGen.String(d[partitionKey] || partitionKey),
      RowKey: entGen.String(d[rowKey])
    })
  })
}

function getValue (field) {
  switch (field.type) {
    case 'app':
      return field.values[0].value.title
    case 'category':
      return field.values[0].value.text
    case 'date':
      return field.values[0].start
    case 'contact':
      return field.values[0].value.name
    default:
      return field.values[0].value
  }
}

function flatten (arr) {
  return arr.reduce((acc, val) => acc.concat(
    Array.isArray(val) ? flatten(val) : val
  ), [])
}

function getPodioAppItems (appId) {
  const callApi = (offset = 0) => api.request('GET', `/item/app/${appId}?limit=500&offset=${offset}`)
  const retriveData = (result = []) => {
    return callApi(result.length)
      .then(data => {
        result = result.concat(data.items)

        if (result.length < data.total) {
          return retriveData(result)
        } else {
          return result
        }
      })
      .catch(err => console.log(err))
  }

  return retriveData()
}

api.authenticateWithCredentials(process.env.USERNAME, process.env.PASSWORD, (err) => {
  if (!err) {
    collectData().then(data => {
      const {coursePool, courseOffer, trainingPlan} = data
      console.log(generateEntities(coursePool, 'Course', 'id')[0])
      console.log(generateEntities(courseOffer, 'Year', 'id')[0])
      console.log(generateEntities(trainingPlan, 'Team', 'id')[0])
    })
  }
})
