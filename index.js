// load env variables
require('dotenv').config()

const azure = require('azure-storage')
const Podio = require('podio-js').api
const config = require('./config')
const camelcase = require('lodash.camelcase')

const api = new Podio({
  authType: 'password',
  clientId: process.env.ID,
  clientSecret: process.env.SECRET
})

const retryOperation = new azure.ExponentialRetryPolicyFilter()
const tableService = azure.createTableService().withFilter(retryOperation)

function collectCoursePool (api) {
  return getPodioAppItems(api, config.course_pool_app_id)
    .then(data => data.map(
      item => item.fields.reduce(
        (acc, val) => Object.assign(
          acc,
          { id: item.item_id },
          { [val.label]: getValue(val) }
        ), {}))
    )
}

function collectCourseOffer (api) {
  return Promise.all(config.offer.map(
    offer => getPodioAppItems(api, offer.app_id)
      .then(data => data.map(
        item => item.fields.reduce(
          (acc, val) => Object.assign(
            acc,
            {
              id: item.item_id,
              year: offer.year
            },
            { [val.label]: getValue(val) }
          ), {})
      ))
  ))
}

function collectTrainingPlan (api) {
  return Promise.all(config.team.map(
    team => getPodioAppItems(api, team.app_id)
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
                    year: team.year,
                    team: team.name,
                    attendeeName: getValue(field)
                  },
                  { [val.label]: getValue(val) }
                ), {})
              )
          )
      ))
  )
}

function collectData (api) {
  return Promise.all([collectCoursePool(api), collectCourseOffer(api), collectTrainingPlan(api)])
    .then(values => {
      const [coursePool, courseOffer, trainingPlan] = values
      return {
        coursePool,
        courseOffer: flatten(courseOffer),
        trainingPlan: flatten(trainingPlan)
      }
    })
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

function uploadToAzure (azure, service, tableName, data) {
  return createAzureTable(service, tableName)
    .then(res => {
      return Promise.all(data.map(d =>
        new Promise((resolve, reject) => {
          service.insertOrReplaceEntity(tableName, d, (err, result, response) => {
            if (err) {
              reject(err)
            } else {
              resolve({ result, response })
            }
          })
        })
      ))
    })
}

function generateEntities (azure, data, partitionKey, rowKey = 'id') {
  const entGen = azure.TableUtilities.entityGenerator

  return data.map(d => {
    const entity = Object.keys(d).reduce((acc, key) => {
      const propertyName = camelcase(key.replace(/[()]/g, ' '))
      if (/^\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d$/.test(d[key])) {
        // suppose this app only used in China office
        acc[propertyName] = entGen.DateTime(new Date(`${d[key]} GMT+0800`))
      } else {
        acc[propertyName] = entGen.String(d[key])
      }

      return acc
    }, {})

    return Object.assign(entity, {
      PartitionKey: entGen.String((d[partitionKey] || partitionKey).toString()),
      RowKey: entGen.String(d[rowKey].toString())
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

function getPodioAppItems (api, appId) {
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
    collectData(api).then(data => {
      const {coursePool, courseOffer, trainingPlan} = data

      return Promise.all([
        uploadToAzure(azure, tableService, 'CoursePool', generateEntities(azure, coursePool, 'Course', 'id')),
        uploadToAzure(azure, tableService, 'CourseOffer', generateEntities(azure, courseOffer, 'Year', 'id')),
        uploadToAzure(azure, tableService, 'TrainingPlan', generateEntities(azure, trainingPlan, 'Team', 'id'))
      ]).then(() => console.log('Done'))
        .catch(err => console.log(err))
    })
  }
})
