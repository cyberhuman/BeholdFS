#ifndef __LOG_H__
#define __LOG_H__

#include <stdarg.h>

void log_init(const char *filename);
void log_free();
void log(const char *format, ...);
void vlog(const char *format, va_list args);

#endif // __LOG_H__
