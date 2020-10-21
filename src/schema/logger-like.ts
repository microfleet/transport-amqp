import * as z from 'zod'

export interface LogFunction {
  (...args: any[]): void
}

export const LogFunction: z.ZodSchema<LogFunction> = z.lazy(() => (
  z.function()
))

export const LoggerLike = z.object({
  info: LogFunction,
  warn: LogFunction,
  trace: LogFunction,
  debug: LogFunction,
  error: LogFunction,
  fatal: LogFunction,
})

export type LoggerLike = z.infer<typeof LoggerLike>
