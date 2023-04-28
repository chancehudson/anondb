/* eslint-disable jest/no-hooks, jest/valid-describe */
import assert from 'assert'
import testSchema from './test-schema'
import { DB, MemoryConnector } from '../src/web'
import { constructSchema } from '../src/types'
import FindTests from './database/find'
import CreateTests from './database/create'
import UpdateTests from './database/update'
import DeleteTests from './database/delete'
import TransactionTests from './database/transaction'

describe('memory tests', function(this: any) {
  this.db = {} as DB
  beforeEach(async () => {
    this.db = new MemoryConnector(constructSchema(testSchema))
    for (const { name } of testSchema) {
      await this.db.delete(name, {
        where: {},
      })
    }
  })

  it('should fail to initialize with duplicate rows', async () => {
    try {
      new MemoryConnector(constructSchema([{
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
          ['id', 'number'],
          ['id', 'number'],
        ]
      }] as any))
    } catch (err) {
      assert(/Duplicate row in table/.test(err.toString()))
    }
  })

  it('should fail to initialize with invalid row type', async () => {
    try {
      new MemoryConnector(constructSchema([{
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
          ['id', 'invalid'],
        ]
      }] as any))
    } catch (err) {
      assert(/Invalid type for row/.test(err.toString()))
    }
  })

  it('should fail to initialize with invalid default value', async () => {
    try {
      new MemoryConnector(constructSchema([{
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
        {
          name: 'id',
          type: 'number',
          default: 'test'
        }
        ]
      }] as any))
    } catch (err) {
      assert(/Default value for row/.test(err.toString()))
    }
    try {
      new MemoryConnector(constructSchema([{
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
        {
          name: 'id',
          type: 'number',
          default: () => 'test'
        }
        ]
      }] as any))
    } catch (err) {
      assert(/Default function for row/.test(err.toString()))
    }
  })

  it('should fail to initialize with duplicate tables', async () => {
    try {
      new MemoryConnector(constructSchema([
      {
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
          ['id', 'number'],
        ]
      },
      {
        name: 'InvalidTable',
        primaryKey: 'id',
        rows: [
          ['id', 'number'],
        ]
      },
      ] as any))
    } catch (err) {
      assert(/Duplicate table name/.test(err.toString()))
    }
  })

  afterEach(async () => {
    await this.db.close()
  })

  FindTests.bind(this)()
  CreateTests.bind(this)()
  UpdateTests.bind(this)()
  DeleteTests.bind(this)()
  TransactionTests.bind(this)()
})
