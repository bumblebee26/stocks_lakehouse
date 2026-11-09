# Shared, non-secret settings for the aws stack. Committed to git so local runs
# and CI/CD apply the same values. (*.auto.tfvars loads automatically and takes
# precedence over terraform.tfvars.) Put secrets in SSM or GitHub secrets, never here.

schedules_enabled = true # NSE + Massive daily fetches
premarket_enabled = true # Telegram brief at 08:45 IST Mon-Fri
fred_enabled      = true # FRED global markets, 06:00 IST Tue-Sat
news_enabled      = true # NSE company news every 30 min, 08:00-19:30 IST Mon-Fri
