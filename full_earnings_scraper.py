#!/usr/bin/env python3
"""
Full Production Earnings Call Scraper
"""

import requests
from bs4 import BeautifulSoup
import sqlite3
import json
import time
import re
from datetime import datetime
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class EarningsCall:
    ticker: str
    company: str
    title: str
    url: str
    call_type: str  # 'webcast', 'audio', 'transcript'
    quarter: str
    year: int
    found_on_page: str
    discovered_at: str

class EarningsDatabase:
    def __init__(self, db_path: str = "earnings_calls.db"):
        self.db_path = db_path
        self.init_db()
    
    def init_db(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS earnings_calls (
                id INTEGER PRIMARY KEY,
                ticker TEXT,
                company TEXT,
                title TEXT,
                url TEXT UNIQUE,
                call_type TEXT,
                quarter TEXT,
                year INTEGER,
                found_on_page TEXT,
                discovered_at TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def save_call(self, call: EarningsCall):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO earnings_calls 
                (ticker, company, title, url, call_type, quarter, year, found_on_page, discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (call.ticker, call.company, call.title, call.url, call.call_type,
                  call.quarter, call.year, call.found_on_page, call.discovered_at))
            
            conn.commit()
            print(f"💾 Saved: {call.ticker} - {call.title}")
        except Exception as e:
            print(f"❌ Error saving: {e}")
        finally:
            conn.close()
    
    def get_stats(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM earnings_calls')
        total = cursor.fetchone()[0]
        
        cursor.execute('SELECT ticker, COUNT(*) FROM earnings_calls GROUP BY ticker')
        by_company = cursor.fetchall()
        
        conn.close()
        
        return {'total': total, 'by_company': by_company}

class FullEarningsScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
        self.db = EarningsDatabase()
    
    def scrape_company(self, ticker: str, company_name: str, ir_urls: List[str]):
        """Scrape all IR URLs for a company"""
        print(f"\n🔍 Scraping {ticker} - {company_name}")
        
        all_calls = []
        
        for url in ir_urls:
            print(f"   Checking: {url}")
            try:
                calls = self._scrape_page(url, ticker, company_name)
                all_calls.extend(calls)
                
                # Look for deeper event pages
                deeper_calls = self._find_event_pages(url, ticker, company_name)
                all_calls.extend(deeper_calls)
                
                time.sleep(1)  # Be respectful
                
            except Exception as e:
                print(f"   ⚠️  Error with {url}: {e}")
                continue
        
        # Save all found calls
        for call in all_calls:
            self.db.save_call(call)
        
        print(f"   ✅ Found {len(all_calls)} earnings calls for {ticker}")
        return all_calls
    
    def _scrape_page(self, url: str, ticker: str, company_name: str) -> List[EarningsCall]:
        """Scrape a single page for earnings content"""
        calls = []
        
        response = self.session.get(url, timeout=10)
        if response.status_code != 200:
            return calls
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Look for various types of earnings links
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text().strip()
            
            call = self._analyze_link(link, href, text, url, ticker, company_name)
            if call:
                calls.append(call)
        
        return calls
    
    def _analyze_link(self, link_element, href: str, text: str, page_url: str, 
                     ticker: str, company_name: str) -> Optional[EarningsCall]:
        """Analyze a link to see if it's an earnings call"""
        
        text_lower = text.lower()
        
        # Earnings indicators
        earnings_keywords = [
            'earnings call', 'quarterly results', 'q1', 'q2', 'q3', 'q4',
            'conference call', 'webcast', 'financial results', 'investor call'
        ]
        
        if not any(keyword in text_lower for keyword in earnings_keywords):
            return None
        
        # Extract quarter and year
        quarter_match = re.search(r'q([1-4])', text_lower)
        year_match = re.search(r'(20\d{2})', text)
        
        quarter = f"Q{quarter_match.group(1)}" if quarter_match else "Unknown"
        year = int(year_match.group(1)) if year_match else datetime.now().year
        
        # Determine call type
        call_type = "webcast"
        if any(ext in href.lower() for ext in ['.mp3', '.wav', '.m4a']):
            call_type = "audio"
        elif any(ext in href.lower() for ext in ['.mp4', '.webm']):
            call_type = "video"
        elif 'transcript' in text_lower:
            call_type = "transcript"
        
        full_url = urljoin(page_url, href)
        
        return EarningsCall(
            ticker=ticker,
            company=company_name,
            title=text,
            url=full_url,
            call_type=call_type,
            quarter=quarter,
            year=year,
            found_on_page=page_url,
            discovered_at=datetime.now().isoformat()
        )
    
    def _find_event_pages(self, base_url: str, ticker: str, company_name: str) -> List[EarningsCall]:
        """Look for dedicated event/webcast pages"""
        calls = []
        
        # Common event page patterns
        domain = urlparse(base_url).netloc
        scheme = urlparse(base_url).scheme
        
        event_urls = [
            f"{scheme}://{domain}/events",
            f"{scheme}://{domain}/events-and-presentations",
            f"{scheme}://{domain}/webcasts",
            f"{scheme}://{domain}/earnings",
            f"{scheme}://{domain}/investor-relations/events"
        ]
        
        for event_url in event_urls:
            try:
                response = self.session.get(event_url, timeout=5)
                if response.status_code == 200:
                    print(f"      📅 Found event page: {event_url}")
                    page_calls = self._scrape_page(event_url, ticker, company_name)
                    calls.extend(page_calls)
            except:
                continue
        
        return calls

def main():
    print("🚀 Full Production Earnings Call Scraper")
    print("=" * 50)
    
    scraper = FullEarningsScraper()
    
    # Major companies with their known IR URLs
    companies = [
        {
            'ticker': 'AAPL',
            'name': 'Apple Inc.',
            'urls': [
                'https://investor.apple.com/investor-relations/default.aspx',
                'https://investor.apple.com/events-and-presentations/default.aspx'
            ]
        },
        {
            'ticker': 'TSLA',
            'name': 'Tesla Inc.',
            'urls': [
                'https://ir.tesla.com/',
                'https://ir.tesla.com/events-and-presentations/events'
            ]
        },
        {
            'ticker': 'GOOGL',
            'name': 'Alphabet Inc.',
            'urls': [
                'https://abc.xyz/investor/',
                'https://abc.xyz/investor/events/'
            ]
        },
        {
            'ticker': 'MSFT',
            'name': 'Microsoft Corporation',
            'urls': [
                'https://www.microsoft.com/en-us/investor/',
                'https://www.microsoft.com/en-us/Investor/events/default.aspx'
            ]
        }
    ]
    
    total_found = 0
    
    for company in companies:
        calls = scraper.scrape_company(
            company['ticker'], 
            company['name'], 
            company['urls']
        )
        total_found += len(calls)
    
    # Show results
    print(f"\n📊 DISCOVERY SUMMARY:")
    print(f"=" * 30)
    
    stats = scraper.db.get_stats()
    print(f"Total earnings calls found: {stats['total']}")
    
    print(f"\nBy company:")
    for ticker, count in stats['by_company']:
        print(f"   {ticker}: {count} calls")
    
    # Export results
    print(f"\n💾 Exporting results...")
    
    # Export to JSON
    conn = sqlite3.connect("earnings_calls.db")
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM earnings_calls')
    rows = cursor.fetchall()
    conn.close()
    
    export_data = []
    for row in rows:
        export_data.append({
            'ticker': row[1],
            'company': row[2],
            'title': row[3],
            'url': row[4],
            'call_type': row[5],
            'quarter': row[6],
            'year': row[7],
            'found_on_page': row[8],
            'discovered_at': row[9]
        })
    
    with open('full_earnings_discovery.json', 'w') as f:
        json.dump(export_data, f, indent=2)
    
    print(f"✅ Results exported to full_earnings_discovery.json")
    print(f"✅ Database saved to earnings_calls.db")
    
    print(f"\n🎉 Full scraping completed!")
    print(f"Found {total_found} earnings calls total")

if __name__ == "__main__":
    main()
