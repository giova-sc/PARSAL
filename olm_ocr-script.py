import os
import glob
import json
import base64
import re
import time
from datetime import datetime
from io import BytesIO
from PIL import Image
import subprocess

# PyPDF2 to count the number of pages in the PDF
from PyPDF2 import PdfReader
from olmocr.data.renderpdf import render_pdf_to_base64png
from olmocr.prompts import build_finetuning_prompt
from olmocr.prompts.anchor import get_anchor_text


os.environ["PATH"] = "/home/gsciortino/.conda/envs/olmocr_env/bin:" + os.environ.get("PATH", "")

# Import PyTorch and HuggingFace modules
import torch
from transformers import AutoProcessor, Qwen2VLForConditionalGeneration

# --- Configuration
# 'mode': 'single' to process a single PDF, or 'batch' to process an entire folder.
# 'input_path': if mode == 'single', it’s the path to the PDF; if mode == 'batch', it’s the folder containing PDFs.
# 'output_path': directory where output files will be saved.

config = {
    "mode": "single",  # 'single' or 'batch'
    "input_path": "/home/gsciortino/DARCH_dataset/Darch/pdf/1.pdf",  # For batch mode: PDF folder; for single: path to one PDF
    "output_path": "/home/gsciortino/OCR/olmOCR/output_text_olmOCR",  # Output directory for .txt files
    "max_new_tokens": 5000,  # Maximum number of tokens generated per page
    "target_longest_image_dim": 1024,  # Maximum dimension for image rendering
    "page_processing": True  # If True, process all pages of the PDF
}

# Initialize olmOCR model
print(f"[{datetime.now()}] Initializing olmOCR model...")
model = Qwen2VLForConditionalGeneration.from_pretrained(
    "allenai/olmOCR-7B-0225-preview", torch_dtype=torch.bfloat16
).eval().to("cuda")
processor = AutoProcessor.from_pretrained("Qwen/Qwen2-VL-7B-Instruct")
print(f"[{datetime.now()}] Model and processor loaded.")


# Process all pages in a PDF
def process_pdf_allpages(pdf_path, max_new_tokens, target_longest_image_dim):
    reader = PdfReader(pdf_path)
    num_pages = len(reader.pages)
    print(f"[{datetime.now()}] The PDF '{os.path.basename(pdf_path)}' contains {num_pages} pages.")
    
    full_text = ""
    for page_number in range(1, num_pages + 1):
        start_page = time.time()
        try:
            image_b64 = render_pdf_to_base64png(pdf_path, page_number, target_longest_image_dim=target_longest_image_dim)
            anchor_text = get_anchor_text(pdf_path, page_number, pdf_engine="pdfreport", target_length=4000)
            prompt = build_finetuning_prompt(anchor_text)
            messages = [{
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{image_b64}"}}
                ]
            }]
            text_input = processor.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            image = Image.open(BytesIO(base64.b64decode(image_b64)))
            inputs = processor(text=[text_input], images=[image], padding=True, return_tensors="pt").to("cuda")
            output = model.generate(**inputs, temperature=0.8, max_new_tokens=max_new_tokens, num_return_sequences=1, do_sample=True)
            prompt_len = inputs["input_ids"].shape[1]
            new_tokens = output[:, prompt_len:]
            raw_page_output = processor.tokenizer.decode(new_tokens[0], skip_special_tokens=True).strip()
            
            try:
                page_data = json.loads(raw_page_output)
                page_text = page_data.get("natural_text", "").strip()
            except Exception as e:
                page_text = raw_page_output
            
            full_text += page_text + "\n\n"
            end_page = time.time()
            print(f"[{datetime.now()}] Page {page_number} processed in {end_page - start_page:.2f} seconds.")
        except Exception as e:
            print(f"[{datetime.now()}] Error on page {page_number} of {pdf_path}: {e}")
            full_text += f"[Error on page {page_number}]\n\n"
    return full_text


# Processing functions: single PDF and batch

def process_single_pdf(pdf_path, output_dir, config):
    extracted_text = process_pdf_allpages(pdf_path, config['max_new_tokens'], config['target_longest_image_dim'])
    pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
    output_text_path = os.path.join(output_dir, f"{pdf_basename}_clean_output.txt")
    with open(output_text_path, "w", encoding="utf-8") as f:
        f.write(extracted_text)
    print(f"[{datetime.now()}] Clean text saved to: {output_text_path}")

def process_batch(input_dir, output_dir, config):
    os.makedirs(output_dir, exist_ok=True)
    pdf_files = glob.glob(os.path.join(input_dir, "*.pdf"))
    print(f"[{datetime.now()}] Found {len(pdf_files)} PDF files in {input_dir}.")
    for pdf_file in pdf_files:
        print(f"[{datetime.now()}] Processing: {pdf_file}")
        start_file = time.time()
        extracted_text = process_pdf_allpages(pdf_file, config['max_new_tokens'], config['target_longest_image_dim'])
        pdf_basename = os.path.splitext(os.path.basename(pdf_file))[0]
        out_file = os.path.join(output_dir, f"{pdf_basename}_clean_output.txt")
        with open(out_file, "w", encoding="utf-8") as f_out:
            f_out.write(extracted_text)
        end_file = time.time()
        print(f"[{datetime.now()}] Extracted text saved to: {out_file}. Time taken: {end_file - start_file:.2f} seconds.\n")


# Main

if __name__ == '__main__':
    os.makedirs(config['output_path'], exist_ok=True)
    mode = config.get('mode', 'single')
    if mode == 'single':
        process_single_pdf(config['input_path'], config['output_path'], config)
    elif mode == 'batch':
        process_batch(config['input_path'], config['output_path'], config)
    else:
        print("Unrecognized mode in config. Use 'single' or 'batch'.")