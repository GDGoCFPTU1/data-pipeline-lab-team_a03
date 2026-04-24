import os
import json
import glob

# Import các thành phần
from schema import UnifiedDocument
from process_unstructured import process_pdf_data, process_video_data
from quality_check import run_semantic_checks

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(BASE_DIR, "..", "raw_data")
OUTPUT_FILE = os.path.join(BASE_DIR, "..", "processed_knowledge_base.json")

def run_pipeline():
    final_kb = []
    
    # Xử lý Group A (PDFs)
    pdf_files = glob.glob(os.path.join(RAW_DATA_DIR, "group_a_pdfs", "*.json"))
    for file_path in pdf_files:
        with open(file_path, 'r') as f:
            raw_data = json.load(f)
        
        # Bước 1: Gọi hàm xử lý PDF (process_pdf_data) từ Role 2
        processed_data = process_pdf_data(raw_data)
        
        # Bước 2: Kiểm tra chất lượng (run_semantic_checks) từ Role 3
        # Nếu đạt (True) thì sử dụng Schema (Role 1) để validate và thêm vào list final_kb
        if run_semantic_checks(processed_data):
            try:
                # Dùng UnifiedDocument để đảm bảo dữ liệu đúng chuẩn trước khi lưu
                doc = UnifiedDocument(**processed_data)
                final_kb.append(doc.model_dump())
            except Exception as e:
                print(f"Schema validation failed for {file_path}: {e}")

    # Xử lý Group B (Videos)
    video_files = glob.glob(os.path.join(RAW_DATA_DIR, "group_b_videos", "*.json"))
    for file_path in video_files:
        with open(file_path, 'r') as f:
            raw_data = json.load(f)
        
        # Gọi hàm xử lý Video từ Role 2
        processed_data = process_video_data(raw_data)
        
        # Kiểm tra chất lượng từ Role 3
        if run_semantic_checks(processed_data):
            try:
                # Dùng UnifiedDocument để đảm bảo dữ liệu đúng chuẩn
                doc = UnifiedDocument(**processed_data)
                final_kb.append(doc.model_dump())
            except Exception as e:
                print(f"Schema validation failed for {file_path}: {e}")

    # Lưu kết quả
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(final_kb, f, indent=4, ensure_ascii=False)
        print(f"Pipeline finished! Saved {len(final_kb)} records to {OUTPUT_FILE}")

if __name__ == "__main__":
    run_pipeline()
