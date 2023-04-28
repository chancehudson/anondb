/* eslint-disable jest/require-top-level-describe, no-plusplus, jest/no-export */
import assert from 'assert'
import { DB } from '../../src'

export default function(this: { db: DB }) {
  test('should create tables', async () => {
    const table = 'TableOne'
    await this.db.create(table, {
      uniqueField: 'testvalue',
      regularField: 'value',
    })
    const doc = await this.db.findOne(table, {
      where: { uniqueField: 'testvalue' },
    })
    assert(doc, 'Inserted document does not exist')
  })

  test('should create documents', async () => {
    const table = 'TableThree'
    {
      const created = await this.db.create(table, {
        id: 'test',
      })
      assert.equal(created.id, 'test')
    }
    {
      const docs = await this.db.create(table, [
        {
          id: 'test1',
        },
        {
          id: 'test2',
          optionalField: 'test',
        },
        {
          id: 'test3',
          optionalField: 'anothertest',
        },
      ])
      assert.equal(docs[0].id, 'test1')
      assert.equal(docs[1].id, 'test2')
      assert.equal(docs[2].id, 'test3')
    }
  })

  test('should execute empty clause', async () => {
    await this.db.create('TableThree', [])
  })

  test('should catch creation type errors', async () => {
    const table = 'Table7'
    try {
      await this.db.create(table, {
        id: 0,
        boolField: '0',
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type boolean/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: 999,
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type boolean/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: {},
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type boolean/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: true,
        boolField: true,
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type number/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: {},
        boolField: true,
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type number/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 'test',
        boolField: true,
        stringField: 'test',
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type number/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: 0,
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type string/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: {},
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type string/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: true,
        bigintField: 1n,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type string/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: 'test',
        bigintField: 1,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type bigint/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: 'test',
        bigintField: 't',
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type bigint/.test(err.toString()))
    }
    try {
      await this.db.create(table, {
        id: 0,
        boolField: true,
        stringField: 'test',
        bigintField: false,
      })
      assert(false)
    } catch (err) {
      assert(/Unrecognized value .* for type bigint/.test(err.toString()))
    }
  })
}
