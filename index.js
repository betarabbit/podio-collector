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
          {
            id: item.item_id,
            courseId: item.app_item_id_formatted,
            createdByUserId: item.created_by.user_id,
            createdByUserName: item.created_by.name,
            createdOn: item.created_on,
            tags: item.tags.join(',')
          },
          getField(val)
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
              OfferId: item.app_item_id_formatted,
              year: offer.year,
              createdByUserId: item.created_by.user_id,
              createdByUserName: item.created_by.name,
              createdOn: item.created_on,
              tags: item.tags.join(',')
            },
            getField(val)
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
            field => field.values
              .map(
                attendee => Object.assign(
                  {
                    id: `${item.item_id}_${attendee.value.user_id}`,
                    planId: `${team.name.toUpperCase()}_${team.year}_${item.app_item_id_formatted}`,
                    year: team.year,
                    team: team.name,
                    createdByUserId: item.created_by.user_id,
                    createdByUserName: item.created_by.name,
                    createdOn: item.created_on,
                    attendeeName: attendee.value.name,
                    attendeeEmail: Array.isArray(attendee.value.mail) && attendee.value.mail.length ? attendee.value.mail[0] : '',
                    tags: item.tags.join(',')
                  },
                  item.fields
                    .filter(field => field.label !== 'Attendee Name')
                    .reduce(
                      (acc, val) => Object.assign(
                        acc,
                        getField(val)
                      ), {})
                )
              )
          )
      ))
    )
  )
}

function collectTeamMembers (api) {
  return Promise.all(config.team.map(
    team => getPodioSpaceMembers(api, team.space_id)
      .then(data => data
        .filter(user => user.profile.name !== 'nkgDataCollector').map(
        user => ({
          id: user.profile.user_id,
          team: team.name,
          name: user.profile.name,
          employee: user.employee,
          mail: Array.isArray(user.profile.mail) && user.profile.mail.length ? user.profile.mail[0] : ''
        })
      ))
  ))
}

function collectData (api) {
  return Promise.all([collectTeamMembers(api), collectCoursePool(api), collectCourseOffer(api), collectTrainingPlan(api)])
    .then(values => {
      console.log('All data colllected!')
      const [teamMember, coursePool, courseOffer, trainingPlan] = values
      return {
        coursePool,
        teamMember: flatten(teamMember),
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
      } else if (typeof d[key] === 'boolean') {
        acc[propertyName] = entGen.Boolean(d[key])
      } else {
        acc[propertyName] = entGen.String(String(d[key]))
      }

      return acc
    }, {})

    return Object.assign(entity, {
      PartitionKey: entGen.String(String(d[partitionKey] || partitionKey)),
      RowKey: entGen.String(String(d[rowKey]))
    })
  })
}

function getField (field) {
  switch (field.type) {
    case 'app':
      return { [field.label]: field.values[0].value.title }
    case 'category':
      return { [field.label]: field.values[0].value.text }
    case 'date':
      return { [field.label]: field.values[0].start }
    case 'contact':
      return { [field.label]: field.values[0].value.name }
    case 'money':
      return {
        [`${field.label}Currency`]: field.values[0].currency,
        [`${field.label}Amount`]: field.values[0].value
      }
    default:
      return { [field.label]: field.values[0].value }
  }
}

function flatten (arr) {
  return arr.reduce((acc, val) => acc.concat(
    Array.isArray(val) ? flatten(val) : val
  ), [])
}

function getPodioAppItems (api, appId) {
  const callApi = (offset = 0) => api.request('GET', `/item/app/${appId}?fields=items.fields(tags)&limit=500&offset=${offset}`)
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

function getPodioSpaceMembers (api, spaceId) {
  return api.request('GET', `/space/${spaceId}/member/v2?limit=500`)
}

api.authenticateWithCredentials(process.env.USERNAME, process.env.PASSWORD, (err) => {
  if (!err) {
    console.log('Start to collect Podio data...')
    collectData(api).then(data => {
      const {teamMember, coursePool, courseOffer, trainingPlan} = data

      console.log('Start to upload data to Azure Table Storage...')
      return Promise.all([
        uploadToAzure(azure, tableService, 'Team', generateEntities(azure, teamMember, 'team', 'id')),
        uploadToAzure(azure, tableService, 'CoursePool', generateEntities(azure, coursePool, 'course', 'id')),
        uploadToAzure(azure, tableService, 'CourseOffer', generateEntities(azure, courseOffer, 'year', 'id')),
        uploadToAzure(azure, tableService, 'TrainingPlan', generateEntities(azure, trainingPlan, 'team', 'id'))
      ]).then(() => console.log('All done!'))
    }).catch(err => console.log(err))
  } else {
    console.log(err)
  }
})
