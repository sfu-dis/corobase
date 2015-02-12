#pragma once
#include <limits>
#define YEAR0           1900                    /* the first year */
#define EPOCH_YR        1970            /* EPOCH = Jan 1 1970 00:00:00 */
#define SECS_DAY        (24L * 60L * 60L)
#define LEAPYEAR(year)  (!((year) % 4) && (((year) % 100) || !((year) % 400)))
#define YEARSIZE(year)  (LEAPYEAR(year) ? 366 : 365)

const int _ytab[2][12] = {
	{ 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 },
	{ 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 }};

struct tm* user_localtime_r(time_t* timer, struct tm* timep)
{
	time_t time = *timer;
	int year = EPOCH_YR;

    register auto dayclock = time % SECS_DAY;
    register auto dayno = time / SECS_DAY;

	timep->tm_sec = dayclock % 60;
	timep->tm_min = (dayclock % 3600) / 60;
	timep->tm_hour = dayclock / 3600;
	timep->tm_wday = (dayno + 4) % 7;       /* day 0 was a thursday */
	while (dayno >= YEARSIZE(year)) {
		dayno -= YEARSIZE(year);
		year++;
	}
	timep->tm_year = year - YEAR0;
	timep->tm_yday = dayno;
	timep->tm_mon = 0;
	while (dayno >= _ytab[LEAPYEAR(year)][timep->tm_mon]) {
		dayno -= _ytab[LEAPYEAR(year)][timep->tm_mon];
		timep->tm_mon++;
	}
	timep->tm_mday = dayno + 1;
	timep->tm_isdst = 0;

	return timep;
}

#if defined(false)
time_t my_mktime( struct tm* timep )
{
	register long day, year;
	register int tm_year;
	int yday, month;
	register uint64_t seconds;
	int overflow;
	unsigned dst;

	timep->tm_min += timep->tm_sec / 60;
	timep->tm_sec %= 60;
	if (timep->tm_sec < 0) {
		timep->tm_sec += 60;
		timep->tm_min--;
	}
	timep->tm_hour += timep->tm_min / 60;
	timep->tm_min = timep->tm_min % 60;
	if (timep->tm_min < 0) {
		timep->tm_min += 60;
		timep->tm_hour--;
	}
	day = timep->tm_hour / 24;
	timep->tm_hour= timep->tm_hour % 24;
	if (timep->tm_hour < 0) {
		timep->tm_hour += 24;
		day--;
	}
	timep->tm_year += timep->tm_mon / 12;
	timep->tm_mon %= 12;
	if (timep->tm_mon < 0) {
		timep->tm_mon += 12;
		timep->tm_year--;
	}
	day += (timep->tm_mday - 1);
	while (day < 0) {
		if(--timep->tm_mon < 0) {
			timep->tm_year--;
			timep->tm_mon = 11;
		}
		day += _ytab[LEAPYEAR(YEAR0 + timep->tm_year)][timep->tm_mon];
	}
	while (day >= _ytab[LEAPYEAR(YEAR0 + timep->tm_year)][timep->tm_mon]) {
		day -= _ytab[LEAPYEAR(YEAR0 + timep->tm_year)][timep->tm_mon];
		if (++(timep->tm_mon) == 12) {
			timep->tm_mon = 0;
			timep->tm_year++;
		}
	}
	timep->tm_mday = day + 1;
	year = EPOCH_YR;
	if (timep->tm_year < year - YEAR0) 			// can make date after 1970
		assert(false);
	seconds = 0;
	day = 0;                        // means days since day 0 now 
	overflow = 0;

	/* Assume that when day becomes negative, there will certainly
	 * be overflow on seconds.
	 * The check for overflow needs not to be done for leapyears
	 * divisible by 400.
	 * The code only works when year (1970) is not a leapyear.
	 */
#if     EPOCH_YR != 1970
#error  EPOCH_YR != 1970
#endif
	tm_year = timep->tm_year + YEAR0;

	if (numeric_limits<int32_t>::max() / 365 < tm_year - year) overflow++;
	day = (tm_year - year) * 365;
	if (numeric_limits<int32_t>::max()- day < (tm_year - year) / 4 + 1) overflow++;
	day += (tm_year - year) / 4
		+ ((tm_year % 4) && tm_year % 4 < year % 4);
	day -= (tm_year - year) / 100
		+ ((tm_year % 100) && tm_year % 100 < year % 100);
	day += (tm_year - year) / 400
		+ ((tm_year % 400) && tm_year % 400 < year % 400);

	yday = month = 0;
	while (month < timep->tm_mon) {
		yday += _ytab[LEAPYEAR(tm_year)][month];
		month++;
	}
	yday += (timep->tm_mday - 1);
	if (day + yday < 0) overflow++;
	day += yday;

	timep->tm_yday = yday;
	timep->tm_wday = (day + 4) % 7;         /* day 0 was thursday (4) */

	seconds = ((timep->tm_hour * 60L) + timep->tm_min) * 60L + timep->tm_sec;

	if ((numeric_limits<time_t>::max()- seconds) / SECS_DAY < day) overflow++;
	seconds += day * SECS_DAY;

	// skipping time zone stuff

	return seconds;
}
#endif
