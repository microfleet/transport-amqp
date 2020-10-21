import * as z from 'zod'

export const CoercedStringArray =
  z.union([
    z.string(),
    z.string().array()
  ])
  .transform(z.string().array(), str => {
    if (Array.isArray(str)) {
      return str
    }

    return [str]
  })

export const CoercedUniqStringArray = CoercedStringArray
  .refine(val => {
    const met = new Set()

    for (const it of val) {
      if (met.has(it)) {
        return false
      }

      met.add(it)
    }

    return true
  })

export type CoercedStringArray = z.infer<typeof CoercedStringArray>
export type CoercedUniqStringArray = z.infer<typeof CoercedUniqStringArray>
