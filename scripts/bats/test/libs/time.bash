# Function: get_current_date_adjusted_by_year
# Description: This function takes a delta (positive or negative) and returns the current year adjusted by that delta.
#
# Parameters:
#   $1 - The delta to adjust the current year by (integer). Positive values increase the year, negative values decrease it.
#
# Returns:
#   The adjusted year based on the provided delta.
#
# Examples:
#   get_current_date_adjusted_by_year 2   # If the current year is 2024, this will return 2026.
#   get_current_date_adjusted_by_year -3  # If the current year is 2024, this will return 2021.
get_current_date_adjusted_by_year() {
    local delta=$1

    current_year=$(date +%Y)

    adjusted_year=$(( current_year + delta ))

    echo "${adjusted_year}"
}

# Function: get_current_date_adjusted_by_month
# Description: This function takes a delta (positive or negative) and returns the current date adjusted by that delta.
#
# Parameters:
#   $1 - The delta (integer) to adjust the date by. Positive values add months, negative values subtract months.
#
# Returns:
#   The adjusted date in <year>_<month> format.
#
# Examples:
#   get_current_date_adjusted_by_month 3   # Returns date of 3 months earlier
#   get_current_date_adjusted_by_month -12   # Returns last year month
get_current_date_adjusted_by_month() {
    local delta=$1

    year=$(date +%Y)
    month=$(date +%m)

    # Calculate the new month and year
    local total_months=$(( year * 12 + month + delta - 1 ))
    local new_year=$(( total_months / 12 ))
    local new_month=$(( total_months % 12 + 1 ))

    # Format the new month to be two digits
    printf -v new_month "%02d" $new_month

    echo "${new_year}_${new_month}"
}

# Function: get_current_date_adjusted_by_quarter
# Description: This function takes a delta (positive or negative) and returns the current date adjusted by that delta.
#
# Parameters:
#   $1 - The delta (integer) to adjust the date by. Positive values add quarters, negative values subtract quarters.
#
# Returns:
#   The adjusted date in <year>_q<quarter> format.
#
# Examples:
#   get_current_date_adjusted_by_quarter 1   # Returns last quarter
get_current_date_adjusted_by_quarter() {
    local delta=$1

    current_year=$(date +%Y)
    current_month=$(date +%m)

    # Determine the current quarter based on the month
    if (( current_month >= 1 && current_month <= 3 )); then
        current_quarter=1
    elif (( current_month >= 4 && current_month <= 6 )); then
        current_quarter=2
    elif (( current_month >= 7 && current_month <= 9 )); then
        current_quarter=3
    else
        current_quarter=4
    fi

    # Calculate the total number of quarters since year 0, adjust by delta, then convert back to year and quarter
    total_quarters=$(( current_year * 4 + current_quarter + delta - 1 ))
    new_year=$(( total_quarters / 4 ))
    new_quarter=$(( total_quarters % 4 + 1 ))

    echo "${new_year}_q${new_quarter}"
}
