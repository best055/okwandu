import tkinter as tk
from tkinter import ttk, messagebox
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from datetime import datetime
import threading
import psycopg2
from psycopg2 import sql

# Exchange rates (normally you would fetch these from an API)
EXCHANGE_RATES = {
    'EUR': 0.93,  # 1 USD = 0.93 EUR
    'GBP': 0.80,  # 1 USD = 0.80 GBP
    'INR': 83.40  # 1 USD = 83.40 INR
}

# PostgreSQL configuration
POSTGRES_CONFIG = {
    'dbname': 'bank_data',
    'user': 'postgres',
    'password': 'best1234',  # Change this to your PostgreSQL password
    'host': 'localhost',
    'port': '5432'
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bank_scraper_log.txt'),
        logging.StreamHandler()
    ]
)

class BankScraperApp:
    def __init__(self, root):
        self.root = root
        self.root.title("World Bank Market Cap Scraper")
        self.root.geometry("1000x700")
        
        # Database configuration
        self.db_engine = None
        self.table_name = "bank_market_cap"
        
        # Create GUI elements
        self.create_widgets()
        
        # Initialize database connection
        self.init_db()
    
    def create_widgets(self):
        """Create all GUI widgets"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title label
        title_label = ttk.Label(main_frame, text="World Bank Market Capitalization Scraper", 
                              font=('Helvetica', 14, 'bold'))
        title_label.grid(row=0, column=0, columnspan=3, pady=10)
        
        # Scrape button
        self.scrape_button = ttk.Button(main_frame, text="Scrape Bank Data", 
                                      command=self.start_scraping_thread)
        self.scrape_button.grid(row=1, column=0, pady=10, sticky=tk.W)
        
        # Load to DB button
        self.load_button = ttk.Button(main_frame, text="Load to PostgreSQL", 
                                    command=self.load_to_db, state=tk.DISABLED)
        self.load_button.grid(row=1, column=1, pady=10)
        
        # Clear DB button
        self.clear_button = ttk.Button(main_frame, text="Clear Database", 
                                     command=self.clear_database)
        self.clear_button.grid(row=1, column=2, pady=10, sticky=tk.E)
        
        # Progress label
        self.progress_label = ttk.Label(main_frame, text="Ready")
        self.progress_label.grid(row=2, column=0, columnspan=3, pady=5)
        
        # Progress bar
        self.progress = ttk.Progressbar(main_frame, orient=tk.HORIZONTAL, 
                                      length=800, mode='determinate')
        self.progress.grid(row=3, column=0, columnspan=3, pady=10)
        
        # Treeview for displaying data
        self.tree = ttk.Treeview(main_frame)
        self.tree.grid(row=4, column=0, columnspan=3, sticky='nsew', pady=10)
        
        # Scrollbars
        y_scroll = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=self.tree.yview)
        y_scroll.grid(row=4, column=3, sticky='ns')
        x_scroll = ttk.Scrollbar(main_frame, orient=tk.HORIZONTAL, command=self.tree.xview)
        x_scroll.grid(row=5, column=0, columnspan=3, sticky='ew')
        
        self.tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)
        
        # Configure grid weights
        main_frame.rowconfigure(4, weight=1)
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.columnconfigure(2, weight=1)
        
        # Store scraped data
        self.bank_data = None
    
    def init_db(self):
        """Initialize PostgreSQL database connection and create table if needed"""
        try:
            # Create connection string
            conn_str = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
            
            # Create SQLAlchemy engine
            self.db_engine = create_engine(conn_str)
            
            # Test connection
            with self.db_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            logging.info(f"Connected to PostgreSQL database: {POSTGRES_CONFIG['dbname']}")
            
            # Check if table exists, if not create it
            inspector = inspect(self.db_engine)
            if not inspector.has_table(self.table_name):
                self.create_table()
                
            self.progress_label.config(text="PostgreSQL database ready")
        except Exception as e:
            logging.error(f"PostgreSQL initialization failed: {str(e)}")
            messagebox.showerror("Database Error", f"Failed to initialize PostgreSQL database: {str(e)}")
    
    def create_table(self):
        """Create the database table if it doesn't exist"""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id SERIAL PRIMARY KEY,
            bank_name VARCHAR(255),
            market_cap_usd NUMERIC(15, 2),
            market_cap_eur NUMERIC(15, 2),
            market_cap_gbp NUMERIC(15, 2),
            market_cap_inr NUMERIC(15, 2),
            scrape_date TIMESTAMP
        )
        """
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            logging.info(f"Created table: {self.table_name}")
        except SQLAlchemyError as e:
            logging.error(f"Failed to create table: {str(e)}")
            raise
    
    def clear_database(self):
        """Clear all data from the database table"""
        try:
            with self.db_engine.connect() as conn:
                conn.execute(text(f"TRUNCATE TABLE {self.table_name} RESTART IDENTITY"))
                conn.commit()
            logging.info(f"Cleared all data from table: {self.table_name}")
            messagebox.showinfo("Success", "Database table cleared successfully!")
        except Exception as e:
            logging.error(f"Failed to clear database: {str(e)}")
            messagebox.showerror("Database Error", f"Failed to clear database: {str(e)}")
    
    def start_scraping_thread(self):
        """Start scraping in a separate thread to keep GUI responsive"""
        self.scrape_button.config(state=tk.DISABLED)
        self.progress_label.config(text="Scraping data...")
        self.progress['value'] = 0
        
        thread = threading.Thread(target=self.scrape_bank_data)
        thread.start()
    
    def scrape_bank_data(self):
        """Scrape bank data from Wikipedia"""
        try:
            logging.info("Starting data scraping")
            url = "https://en.wikipedia.org/wiki/List_of_largest_banks"
            
            # Update progress
            self.update_progress(10, "Fetching webpage...")
            
            # Fetch the webpage
            response = requests.get(url)
            response.raise_for_status()
            
            # Parse HTML
            self.update_progress(30, "Parsing HTML...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the table - adjust selector as needed
            tables = soup.find_all('table', {'class': 'wikitable'})
            if not tables:
                raise ValueError("Could not find any tables with class 'wikitable'")
            
            # We'll use the first table (adjust index if needed)
            table = tables[0]
            
            # Extract table data
            self.update_progress(50, "Extracting table data...")
            rows = table.find_all('tr')[1:]  # Skip header row
            
            data = []
            for row in rows:
                cols = row.find_all('td')
                if len(cols) >= 3:  # Ensure we have enough columns
                    rank = cols[0].text.strip()
                    bank_name = cols[1].text.strip()
                    market_cap_str = cols[2].text.strip()
                    
                    # Clean market cap value (remove $ and billion)
                    try:
                        market_cap = float(market_cap_str.replace('$', '').replace(' billion', '').strip())
                    except ValueError:
                        continue  # Skip rows with invalid market cap values
                    
                    data.append({
                        'Rank': rank,
                        'Bank': bank_name,
                        'Market Cap (USD Billion)': market_cap
                    })
            
            if not data:
                raise ValueError("No data extracted from the table")
            
            # Convert to DataFrame
            self.update_progress(70, "Processing data...")
            df = pd.DataFrame(data)
            
            # Convert to other currencies
            for currency, rate in EXCHANGE_RATES.items():
                df[f'Market Cap ({currency} Billion)'] = (df['Market Cap (USD Billion)'] * rate).round(2)
            
            self.bank_data = df
            
            # Display in Treeview
            self.display_data(df)
            
            self.update_progress(100, "Scraping complete!")
            self.load_button.config(state=tk.NORMAL)
            logging.info(f"Data scraping completed successfully. Found {len(df)} banks.")
            
        except Exception as e:
            logging.error(f"Scraping failed: {str(e)}")
            self.update_progress(0, f"Error: {str(e)}")
            messagebox.showerror("Scraping Error", f"Failed to scrape data: {str(e)}")
        finally:
            self.scrape_button.config(state=tk.NORMAL)
    
    def display_data(self, df):
        """Display data in the Treeview widget"""
        # Clear existing data
        for item in self.tree.get_children():
            self.tree.delete(item)
        
        # Set up columns
        self.tree['columns'] = list(df.columns)
        self.tree['show'] = 'headings'
        
        # Create headings
        for col in df.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, anchor=tk.CENTER)
        
        # Add data
        for _, row in df.iterrows():
            self.tree.insert('', tk.END, values=list(row))
    
    def load_to_db(self):
        """Load scraped data to PostgreSQL database"""
        if self.bank_data is None:
            messagebox.showwarning("No Data", "No data to load. Please scrape data first.")
            return
        
        try:
            self.progress_label.config(text="Loading data to PostgreSQL...")
            self.progress['value'] = 0
            self.load_button.config(state=tk.DISABLED)
            
            # Add scrape date
            df = self.bank_data.copy()
            df['scrape_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Rename columns to match database
            df = df.rename(columns={
                'Bank': 'bank_name',
                'Market Cap (USD Billion)': 'market_cap_usd',
                'Market Cap (EUR Billion)': 'market_cap_eur',
                'Market Cap (GBP Billion)': 'market_cap_gbp',
                'Market Cap (INR Billion)': 'market_cap_inr'
            })
            
            # Select only the columns we need for the database
            db_columns = ['bank_name', 'market_cap_usd', 'market_cap_eur', 
                         'market_cap_gbp', 'market_cap_inr', 'scrape_date']
            df = df[db_columns]
            
            # Load to database in chunks with progress updates
            total_rows = len(df)
            chunk_size = 10
            chunks = [df[i:i + chunk_size] for i in range(0, total_rows, chunk_size)]
            
            for i, chunk in enumerate(chunks):
                # Update progress
                progress = (i / len(chunks)) * 100
                self.update_progress(progress, f"Loading {len(chunk)} records...")
                
                # Write to PostgreSQL
                chunk.to_sql(
                    self.table_name,
                    self.db_engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
            
            self.update_progress(100, "Data loaded to PostgreSQL successfully!")
            logging.info(f"Successfully loaded {total_rows} records to PostgreSQL")
            messagebox.showinfo("Success", f"Data loaded to PostgreSQL successfully!\n{total_rows} records inserted.")
            
        except Exception as e:
            logging.error(f"Failed to load data to PostgreSQL: {str(e)}")
            self.update_progress(0, f"Error: {str(e)}")
            messagebox.showerror("Database Error", f"Failed to load data: {str(e)}")
        finally:
            self.load_button.config(state=tk.NORMAL)
    
    def update_progress(self, value, message):
        """Update progress bar and label"""
        self.progress['value'] = value
        self.progress_label.config(text=message)
        self.root.update_idletasks()

if __name__ == "__main__":
    root = tk.Tk()
    app = BankScraperApp(root)
    root.mainloop()