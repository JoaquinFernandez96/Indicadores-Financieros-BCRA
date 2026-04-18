import os
from fpdf import FPDF
from datetime import datetime

class PDFReport(FPDF):
    def __init__(self, cliente_nombre, periodo, ref_mercado):
        super().__init__()
        self.cliente_nombre = cliente_nombre
        self.periodo = periodo
        self.ref_mercado = ref_mercado
        self.set_auto_page_break(auto=True, margin=15)
        
    def header(self):
        # Fondo sutil para el header (Opcional: Rectángulo azul cobalto muy claro)
        self.set_fill_color(0, 71, 171) # Azul Cobalto
        self.rect(0, 0, 210, 25, 'F')
        
        # Blanco para el texto en el header
        self.set_text_color(255, 255, 255)
        self.set_font('helvetica', 'B', 16)
        self.cell(0, 10, f"Reporte de Benchmarking Financiero BCRA", ln=True, align='L')
        
        self.set_font('helvetica', '', 10)
        self.cell(0, 5, f"Entidad: {self.cliente_nombre} | Período: {self.periodo}", ln=True, align='L')
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font('helvetica', 'I', 8)
        self.set_text_color(128, 128, 128)
        fecha_gen = datetime.now().strftime("%d/%m/%Y %H:%M")
        self.cell(0, 10, f'Generado el: {fecha_gen} | Confidencial · BCRA Dashboard', 0, 0, 'L')
        self.cell(0, 10, f'Página {self.page_no()}/{{nb}}', 0, 0, 'R')

    def section_title(self, label, icon_name=None):
        self.set_text_color(30, 41, 59) # Slate 800
        self.set_font('helvetica', 'B', 14)
        self.ln(5)
        self.cell(0, 10, label, ln=True)
        # Línea de separación
        self.set_draw_color(226, 232, 240)
        self.line(self.get_x(), self.get_y(), 200, self.get_y())
        self.ln(5)

    def add_kpis(self, kpi_data):
        """ kpi_data: list of dicts {label, value, delta, status} """
        self.set_font('helvetica', 'B', 11)
        # Dibujar 5 tarjetas
        x_start = 10
        y_start = self.get_y()
        width = 37
        height = 25
        
        for i, kpi in enumerate(kpi_data):
            # Tarjeta (Borde y Fondo)
            self.set_fill_color(248, 250, 252)
            self.set_draw_color(241, 245, 249)
            self.rect(x_start + (i * (width + 2)), y_start, width, height, 'FD')
            
            # Texto
            self.set_xy(x_start + (i * (width + 2)) + 2, y_start + 2)
            self.set_text_color(100, 116, 139) # Secondary
            self.set_font('helvetica', 'B', 8)
            self.cell(width-4, 5, kpi['label'], align='C', ln=True)
            
            self.set_x(x_start + (i * (width + 2)) + 2)
            self.set_text_color(30, 41, 59) # Primary
            self.set_font('helvetica', 'B', 12)
            self.cell(width-4, 8, kpi['value'], align='C', ln=True)
            
            # Delta
            self.set_x(x_start + (i * (width + 2)) + 2)
            if kpi.get('delta'):
                color = (22, 163, 74) if kpi['status'] == 'normal' else (220, 38, 38)
                self.set_text_color(*color)
                self.set_font('helvetica', '', 7)
                self.cell(width-4, 5, f"{kpi['delta']} vs Ref", align='C', ln=True)
        
        self.set_y(y_start + height + 10)

def generate_pdf_report(cliente, periodo, ref_mercado, kpis, image_paths):
    pdf = PDFReport(cliente, periodo, ref_mercado)
    pdf.alias_nb_pages()
    pdf.add_page()
    
    # KPIs
    pdf.section_title("Resumen de Indicadores Clave")
    pdf.add_kpis(kpis)
    
    # Gráficos
    pdf.section_title("Comparativa de Mercado (Valores Absolutos)")
    if 'bars' in image_paths:
        pdf.image(image_paths['bars'], x=10, w=190)
        pdf.ln(5)
        
    pdf.add_page()
    pdf.section_title("Radar de Desempeño Relativo")
    if 'radar' in image_paths:
        # Centrar radar (es más cuadrado)
        pdf.image(image_paths['radar'], x=30, w=150)
    
    pdf.ln(10)
    pdf.section_title("Evolución del Indicador Clave")
    if 'trend' in image_paths:
        pdf.image(image_paths['trend'], x=10, w=190)

    return bytes(pdf.output())
