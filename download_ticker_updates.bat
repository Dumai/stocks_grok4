@echo off
python "C:\Users\Wes\OneDrive\stocks_grok4\download_stocks.py" > "C:\Users\Wes\OneDrive\stocks_grok4\script_log.txt" 2>&1
echo Script completed at %DATE% %TIME% >> "C:\Users\Wes\OneDrive\stocks_grok4\script_log.txt"
:: pause  (commented out for background runs)