import pandas as pd
from unidecode import unidecode
import os

# --- 1. CONFIGURAÇÃO ---
INPUT_DIR = 'dados_brutos'
OUTPUT_DIR = 'dados_limpos'
TRANSLATION_FILE = 'product_category_name_translation.csv'

# Mapeamento de arquivos
FILE_MAPPING = {
    'olist_geolocation_dataset': {'pk': ['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'], 'cols_str': ['geolocation_city', 'geolocation_state']},
    'olist_products_dataset': {'pk': ['product_id'], 'cols_str': ['product_category_name']},
    'olist_sellers_dataset': {'pk': ['seller_id'], 'cols_str': ['seller_city', 'seller_state']},
    'olist_customers_dataset': {'pk': ['customer_id'], 'cols_str': ['customer_city', 'customer_state']},
    'olist_orders_dataset': {'pk': ['order_id'], 'date_cols': ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']},
    'olist_order_items_dataset': {'pk': ['order_id', 'order_item_id']},
    'olist_order_payments_dataset': {'pk': ['order_id', 'payment_sequential']},
    'olist_order_reviews_dataset': {'pk': ['review_id'], 'date_cols': ['review_creation_date', 'review_answer_timestamp']}
}

# Armazena IDs válidos para validação cruzada (Sets para busca rápida O(1))
valid_ids = {
    'product_id': set(),
    'seller_id': set(),
    'customer_id': set(),
    'order_id': set(),
    'zip_code': set() # Opcional: para validar geolocalização
}

# --- 2. FUNÇÕES AUXILIARES ---

def load_data(filename):
    path = os.path.join(INPUT_DIR, f"{filename}.csv")
    try:
        return pd.read_csv(path, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding='latin-1')
    except FileNotFoundError:
        print(f"ERRO CRÍTICO: {filename}.csv não encontrado.")
        return None

def clean_text(df, columns):
    """Padroniza texto (mantém acentos para legibilidade, remove espaços, lower)."""
    for col in columns:
        if col in df.columns:
            # Converte para string, lowercase e remove espaços extras
            df[col] = df[col].astype(str).str.strip().str.lower()
            # Opcional: Remover acentos se estritamente necessário para chaves
            # df[col] = df[col].apply(lambda x: unidecode(x) if pd.notna(x) else None)
    return df

def clean_zip_codes(df, col_name):
    """Garante que CEP seja string com 5 dígitos (zeros à esquerda)."""
    if col_name in df.columns:
        # Remove caracteres não numéricos (hífen) e preenche com zeros
        df[col_name] = df[col_name].astype(str).str.replace(r'\D', '', regex=True)
        df[col_name] = df[col_name].str.zfill(5)
    return df

def clean_numerics(df):
    """Trata decimais e inteiros."""
    # Monetários
    for col in ['price', 'freight_value', 'payment_value']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
    
    # Inteiros Gerais (EXCETO CEPs e IDs que são Strings)
    int_cols = ['payment_installments', 'product_photos_qty', 'product_weight_g', 
                'product_length_cm', 'product_height_cm', 'product_width_cm', 
                'review_score', 'order_item_id', 'payment_sequential']
    
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
            
    return df

def apply_translation(df, translation_df):
    """Traduz categorias de produtos se disponível."""
    if 'product_category_name' in df.columns and translation_df is not None:
        df = pd.merge(df, translation_df, left_on='product_category_name', right_on='original', how='left')
        df['product_category_name'] = df['translated'].fillna(df['product_category_name'])
        df.drop(columns=['original', 'translated'], inplace=True)
    return df

# --- 3. PROCESSAMENTO HIERÁRQUICO ---

def process_pipeline():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    # Carregar Tradução
    trans_path = os.path.join(INPUT_DIR, TRANSLATION_FILE)
    translation_df = None
    if os.path.exists(trans_path):
        translation_df = pd.read_csv(trans_path)
        translation_df.columns = ['original', 'translated']
        translation_df['original'] = translation_df['original'].astype(str).str.strip().str.lower()

    # ORDEM DE EXECUÇÃO IMPORTA (Pais -> Filhos -> Netos)
    
    # --- GRUPO 1: DADOS MESTRES (Independentes) ---
    print("\n[FASE 1] Processando Tabelas Mestres...")
    
    # 1.1 Geolocalização
    df_geo = load_data('olist_geolocation_dataset')
    if df_geo is not None:
        df_geo = clean_zip_codes(df_geo, 'geolocation_zip_code_prefix')
        df_geo = clean_text(df_geo, ['geolocation_city', 'geolocation_state'])
        # Remove duplicatas exatas de coordenadas para aliviar o banco
        df_geo.drop_duplicates(subset=['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng'], inplace=True)
        # Salva CEPs válidos
        valid_ids['zip_code'] = set(df_geo['geolocation_zip_code_prefix'].unique())
        df_geo.to_csv(os.path.join(OUTPUT_DIR, 'geolocation.csv'), index=False)
        print(f"  -> Geo: {len(df_geo)} registros.")

    # 1.2 Produtos
    df_prod = load_data('olist_products_dataset')
    if df_prod is not None:
        df_prod = clean_text(df_prod, ['product_category_name'])
        df_prod = apply_translation(df_prod, translation_df)
        df_prod = clean_numerics(df_prod)
        valid_ids['product_id'] = set(df_prod['product_id'].unique())
        df_prod.to_csv(os.path.join(OUTPUT_DIR, 'product.csv'), index=False)
        print(f"  -> Produtos: {len(df_prod)} registros.")

    # 1.3 Vendedores
    df_sel = load_data('olist_sellers_dataset')
    if df_sel is not None:
        df_sel = clean_zip_codes(df_sel, 'seller_zip_code_prefix')
        df_sel = clean_text(df_sel, ['seller_city', 'seller_state'])
        valid_ids['seller_id'] = set(df_sel['seller_id'].unique())
        df_sel.to_csv(os.path.join(OUTPUT_DIR, 'seller.csv'), index=False)
        print(f"  -> Vendedores: {len(df_sel)} registros.")


    # --- GRUPO 2: CLIENTES (Dependem ligeiramente de Geo, mas são Pais de Pedidos) ---
    print("\n[FASE 2] Processando Clientes...")
    
    df_cust = load_data('olist_customers_dataset')
    if df_cust is not None:
        df_cust = clean_zip_codes(df_cust, 'customer_zip_code_prefix')
        df_cust = clean_text(df_cust, ['customer_city', 'customer_state'])
        
        # NOTA: Não filtraremos clientes se o CEP não existir em Geo para não perder vendas,
        # pois o dataset de Geo é amostral. Mas garantimos unicidade.
        df_cust.drop_duplicates(subset=['customer_id'], inplace=True)
        
        valid_ids['customer_id'] = set(df_cust['customer_id'].unique())
        df_cust.to_csv(os.path.join(OUTPUT_DIR, 'customer.csv'), index=False)
        print(f"  -> Clientes: {len(df_cust)} registros.")


    # --- GRUPO 3: PEDIDOS (Dependem de Clientes) ---
    print("\n[FASE 3] Processando Pedidos...")
    
    df_ord = load_data('olist_orders_dataset')
    if df_ord is not None:
        for col in FILE_MAPPING['olist_orders_dataset']['date_cols']:
            df_ord[col] = pd.to_datetime(df_ord[col], errors='coerce')
        
        # --- VALIDAÇÃO CRUZADA: Remover pedidos de clientes inexistentes ---
        initial_len = len(df_ord)
        df_ord = df_ord[df_ord['customer_id'].isin(valid_ids['customer_id'])]
        removed = initial_len - len(df_ord)
        if removed > 0: print(f"  AVISO: {removed} pedidos removidos (Cliente não encontrado).")

        valid_ids['order_id'] = set(df_ord['order_id'].unique())
        df_ord.to_csv(os.path.join(OUTPUT_DIR, 'order.csv'), index=False)
        print(f"  -> Pedidos: {len(df_ord)} registros.")


    # --- GRUPO 4: ITENS E DETALHES (Dependem de Pedidos, Produtos e Vendedores) ---
    print("\n[FASE 4] Processando Itens e Detalhes...")

    # 4.1 Order Items
    df_item = load_data('olist_order_items_dataset')
    if df_item is not None:
        df_item = clean_numerics(df_item)
        
        # Validação Cruzada Tripla (Pedido, Produto, Vendedor)
        initial_len = len(df_item)
        mask = (
            df_item['order_id'].isin(valid_ids['order_id']) &
            df_item['product_id'].isin(valid_ids['product_id']) &
            df_item['seller_id'].isin(valid_ids['seller_id'])
        )
        df_item = df_item[mask]
        
        removed = initial_len - len(df_item)
        if removed > 0: print(f"  AVISO: {removed} itens removidos (Inconsistência de FK).")
        
        df_item.to_csv(os.path.join(OUTPUT_DIR, 'order_item.csv'), index=False)

    # 4.2 Order Payments
    df_pay = load_data('olist_order_payments_dataset')
    if df_pay is not None:
        df_pay = clean_numerics(df_pay)
        # Validação Cruzada (Apenas Pedido)
        df_pay = df_pay[df_pay['order_id'].isin(valid_ids['order_id'])]
        df_pay.to_csv(os.path.join(OUTPUT_DIR, 'order_payment.csv'), index=False)

    # 4.3 Order Reviews
    df_rev = load_data('olist_order_reviews_dataset')
    if df_rev is not None:
        # Corrige quebra de linha em comentários que quebram CSVs
        if 'review_comment_message' in df_rev.columns:
            df_rev['review_comment_message'] = df_rev['review_comment_message'].astype(str).str.replace('\n', ' ')
        
        # Datas e Numéricos
        for col in ['review_creation_date', 'review_answer_timestamp']:
            df_rev[col] = pd.to_datetime(df_rev[col], errors='coerce')
        df_rev['review_score'] = pd.to_numeric(df_rev['review_score'], errors='coerce').astype('Int64')

        # Validação Cruzada (Apenas Pedido)
        # Nota: Reviews podem não ter order_id válidos no dataset original, é vital limpar.
        df_rev = df_rev[df_rev['order_id'].isin(valid_ids['order_id'])]
        
        # Desduplicação de Review ID (Dataset tem duplicatas de reviews)
        df_rev.drop_duplicates(subset=['review_id'], inplace=True)
        
        df_rev.to_csv(os.path.join(OUTPUT_DIR, 'order_review.csv'), index=False)
        print("  -> Itens, Pagamentos e Reviews processados.")

    print("\nSUCESSO: Todos os arquivos foram limpos e validados.")

if __name__ == '__main__':
    process_pipeline()