
#include <stdio.h>
#include <stdarg.h>

#include "log.h"

static FILE *logfile;

void log_init(const char *filename)
{
	logfile = fopen(filename, "a");
}

void log_free()
{
	fclose(logfile);
}

void log(const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	log(format, ap);
	va_end(ap);
}

void vlog(const char *format, va_list args)
{
	if (logfile)
		vfprintf(logfile, format, args);
}

