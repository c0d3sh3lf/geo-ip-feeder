import { writeFileSync, readFileSync } from 'fs'
import { join, resolve } from 'path'
import { config } from 'dotenv'
import csvParser from 'csv-parser'
import { MongoClient } from 'mongodb'
import fetch from 'node-fetch'
import { Readable } from 'stream'

config()
const destinationFolder = process.env.DESTINATION_FOLDER
const asnIPv4CountryCSVFile = join(destinationFolder, 'asnIPv4country.csv')
const asnIPv6CountryCSVFile = join(destinationFolder, 'asnIPv6country.csv')
const asnIPv4CSVFile = join(destinationFolder, 'asnIPv4.csv')
const asnIPv6CSVFile = join(destinationFolder, 'asnIPv6.csv')
const ISO3166CountryCSVFile = join(destinationFolder, 'ISO3166Country.csv')

async function downloadGitRepo() {
  const asnIPv4Country = process.env.ASN_IPV4_CONT
  const asnIPv6Country = process.env.ASN_IPV6_CONT
  const asnIPv4 = process.env.ASN_IPV4
  const asnIPv6 = process.env.ASN_IPV6
  const ISO3166Country = process.env.ISO_3166_CONT

  if (
    !asnIPv4Country ||
    !asnIPv4 ||
    !asnIPv6Country ||
    !asnIPv6 ||
    !destinationFolder
  ) {
    console.error(
      'Download URLs or Destination folder is not defined in the .env file.'
    )
    process.exit(1)
  }

  try {
    //Download ASN IPv4 Country Data
    console.log(`Fetching ASN IPv4 Country Data`)
    const responseIPv4Country = await fetch(asnIPv4Country)
    if (!responseIPv4Country.ok) {
      throw new Error(
        `Failed to fetch ${asnIPv4Country}: ${responseIPv4Country.status} ${responseIPv4Country.statusText}`
      )
    }
    const asnIPv4CountryCSVData = await responseIPv4Country.text()
    const asnIPv4CountryLocalPath = resolve(asnIPv4CountryCSVFile)

    writeFileSync(asnIPv4CountryLocalPath, asnIPv4CountryCSVData)
    console.log(`IPv4 Country Data written to ${asnIPv4CountryLocalPath}`)
  } catch (err) {
    console.error(`Failed to download CSV file`, err)
  }

  try {
    //Download ASN IPv6 Country Data
    console.log(`Fetching ASN IPv6 Country Data`)
    const responseIPv6Country = await fetch(asnIPv6Country)
    if (!responseIPv6Country.ok) {
      throw new Error(
        `Failed to fetch ${asnIPv6Country}: ${responseIPv6Country.status} ${responseIPv6Country.statusText}`
      )
    }
    const asnIPv6CountryCSVData = await responseIPv6Country.text()
    const asnIPv6CountryLocalPath = resolve(asnIPv6CountryCSVFile)

    writeFileSync(asnIPv6CountryLocalPath, asnIPv6CountryCSVData)
    console.log(`IPv6 Country Data written to ${asnIPv6CountryLocalPath}`)
  } catch (err) {
    console.error(`Failed to download CSV file`, err)
  }

  try {
    //Download ASN IPv4 Data
    console.log(`Fetching ASN IPv4 Data`)
    const responseIPv4 = await fetch(asnIPv4)
    if (!responseIPv4.ok) {
      throw new Error(
        `Failed to fetch ${asnIPv4}: ${responseIPv4.status} ${responseIPv4.statusText}`
      )
    }
    const asnIPv4CSVData = await responseIPv4.text()
    const asnIPv4LocalPath = resolve(asnIPv4CSVFile)

    writeFileSync(asnIPv4LocalPath, asnIPv4CSVData)
    console.log(`IPv4 Data written to ${asnIPv4LocalPath}`)
  } catch (err) {
    console.error(`Failed to download CSV file`, err)
  }

  try {
    //Download ASN IPv6 Data
    console.log(`Fetching ASN IPv6 Data`)
    const responseIPv6 = await fetch(asnIPv6)
    if (!responseIPv6.ok) {
      throw new Error(
        `Failed to fetch ${asnIPv6}: ${responseIPv6.status} ${responseIPv6.statusText}`
      )
    }
    const asnIPv6CSVData = await responseIPv6.text()
    const asnIPv6LocalPath = resolve(asnIPv6CSVFile)

    writeFileSync(asnIPv6LocalPath, asnIPv6CSVData)
    console.log(`IPv6 Data written to ${asnIPv6LocalPath}`)
  } catch (err) {
    console.error(`Failed to download CSV file`, err)
  }

  try {
    //Download ISO 3188 Country Data
    console.log(`Fetching ASN IPv6 Data`)
    const responseISO3166Country = await fetch(ISO3166Country)
    if (!responseISO3166Country.ok) {
      throw new Error(
        `Failed to fetch ${ISO3166Country}: ${responseISO3166Country.status} ${responseISO3166Country.statusText}`
      )
    }
    const ISO3166CountryCSVData = await responseISO3166Country.text()
    const ISO3166CountryLocalPath = resolve(ISO3166CountryCSVFile)

    writeFileSync(ISO3166CountryLocalPath, ISO3166CountryCSVData)
    console.log(`IPv6 Data written to ${ISO3166CountryLocalPath}`)
  } catch (err) {
    console.error(`Failed to download CSV file`, err)
  }
}

async function updateMongo() {
  const mongoUri = process.env.MONGO_URI || 'mongodb://localhost:27017'
  const dbName = process.env.DB_NAME || 'testDB'
  const ipv4CollectionName = process.env.IPV4_COLLECTION
  const ipv6CollectionName = process.env.IPV6_COLLECTION

  const client = new MongoClient(mongoUri)

  try {
    await client.connect()
    const db = client.db(dbName)
    const ipv4Collection = db.collection(ipv4CollectionName)
    const ipv6Collection = db.collection(ipv6CollectionName)

    const countries = []

    //read country csv file
    const iso3166CountryCSVData = readFileSync(ISO3166CountryCSVFile, 'utf-8')
    const iso3166CountryStream = Readable.from(iso3166CountryCSVData)
    iso3166CountryStream
      .pipe(csvParser())
      .on('data', (row) => {
        const country = {
          name: row['name'],
          alpha2: row['alpha-2'],
          alpha3: row['alpha-3'],
          countryCode: row['country-code'],
          iso3166_2: row['iso_3166-2'],
          region: row['region'],
          subRegion: row['sub-region'],
          intermediateRegion: row['intermediate-region'],
          regionCode: row['region-code'],
          subRegionCode: row['sub-region-code'],
          intermediateRegionCode: row['intermediate-region-code'],
        }
        countries.push(country)
      })
      .on('end', () => {
        console.log(countries.find((country) => country.alpha2 === 'AE'))
        iso3166CountryStream.destroy()
      })
      .on('error', (err) => {
        console.log(`Error occurred ${err.message}`)
        iso3166CountryStream.destroy()
      })

    //read IPv4 file
  } catch (err) {
    console.error('Failed to connect to MongoDB:', err)
  } finally {
    await client.close()
  }
}

// await downloadGitRepo()
await updateMongo()
