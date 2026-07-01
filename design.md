# Pipeline Design

## Prompt

```text
You've joined a small team at a financial data company.
The product needs to know about thousands of investment firms, allocators, funds, and the people who work at them — globally.
The data lives in dozens of very different sources: 
- regulator filings (SEC, IRS, and equivalents in several countries), 
- news APIs, 
- RSS feeds, 
- third-party data vendors, and firm-published documents — PDFs, spreadsheets, web pages. 

Some you download, some you scrape, some have APIs.

Today it's manual. Analysts pull files, copy them into spreadsheets, and load them. 
We want to automate this end-to-end. 
Walk me through how you'd design it.
```

## Questions

- Is real time or near real time needed? - Assume not
- What matters most traceability or speed? - Traceability
- Do you need human in the loop review for low confidence answers or do you want it to be automated? - HUman in the loop

## Notes

- Unstrcutured and semi strucutured (OCR/NLP)?
- Multi-source points to medalion
- current process uses EUCs
- Process appears to be daily batch oriented