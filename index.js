// load env variables
require('dotenv').config()

const Podio = require('podio-js').api
const config = require('./config')

const api = new Podio({
  authType: 'password',
  clientId: process.env.ID,
  clientSecret: process.env.SECRET
})

function collectCoursePool (api) {
  return api.request('GET', `/item/app/${config.course_pool_app_id}`)
    .then(data => data.items.map(
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
    offer => api.request('GET', `/item/app/${offer.app_id}`)
      .then(data => data.items.map(
        item => item.fields.reduce(
          (acc, val) => Object.assign(
            acc,
            { id: item.item_id, Year: offer.year },
            { [val.label]: getValue(val) }
          ), {})
      ))
  ))
}

function collectTrainingPlan (api) {
  return Promise.all(config.team.map(
    team => api.request('GET', `/item/app/${team.app_id}`)
      .then(data => data.items.map(
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

function uploadToAzure (data) {

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

api.authenticateWithCredentials(process.env.USERNAME, process.env.PASSWORD, (err) => {
  if (!err) {
    collectData(api).then()
  }
})
