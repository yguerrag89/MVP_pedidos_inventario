PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- =========================================================
-- USUARIOS
-- =========================================================
CREATE TABLE IF NOT EXISTS usuarios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    display_name TEXT,
    password_hash TEXT,
    role TEXT NOT NULL DEFAULT 'admin'
        CHECK (role IN ('admin', 'viewer')),
    is_active INTEGER NOT NULL DEFAULT 1
        CHECK (is_active IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- IMPORTACIONES
-- =========================================================
CREATE TABLE IF NOT EXISTS import_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_type TEXT NOT NULL
        CHECK (import_type IN (
            'CARGA_INICIAL',
            'PXS_LIMPIO',
            'ENTRADAS_PRODUCCION'
        )),
    file_name TEXT NOT NULL,
    file_path TEXT,
    file_hash TEXT,
    status TEXT NOT NULL DEFAULT 'PENDIENTE'
        CHECK (status IN (
            'PENDIENTE',
            'PROCESANDO',
            'COMPLETADO',
            'COMPLETADO_CON_REVISION',
            'FALLIDO',
            'ANULADO'
        )),
    total_rows INTEGER NOT NULL DEFAULT 0,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    updated_rows INTEGER NOT NULL DEFAULT 0,
    error_rows INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    created_by TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

-- =========================================================
-- PEDIDOS
-- Reglas clave del MVP:
-- - fecha_produccion existe a nivel pedido y línea
-- - estatus_venta se separa de estado_operativo
-- - los cierres deben mover a enviados sin perder trazabilidad
-- - se deja soporte para relacionar pedidos derivados posteriores
-- =========================================================
CREATE TABLE IF NOT EXISTS pedidos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    id_pedido TEXT NOT NULL UNIQUE,
    serie_documento TEXT,
    documento TEXT,
    tipo_registro TEXT NOT NULL DEFAULT 'PEDIDO_NORMAL'
        CHECK (tipo_registro IN ('PEDIDO_NORMAL', 'VENTA_DIRECTA')),
    origen_registro TEXT NOT NULL DEFAULT 'OTRO'
        CHECK (origen_registro IN (
            'CARGA_INICIAL',
            'PXS_LIMPIO',
            'ALTA_MANUAL',
            'OTRO'
        )),
    import_job_id INTEGER,
    pedido_origen_id INTEGER,
    cliente TEXT,
    fecha_captura TEXT,
    fecha_produccion TEXT,
    fecha_logistica TEXT,
    estatus_venta TEXT,
    prioridad_manual INTEGER,
    porcentaje_asignacion REAL NOT NULL DEFAULT 0
        CHECK (porcentaje_asignacion >= 0 AND porcentaje_asignacion <= 100),
    estado_operativo TEXT NOT NULL DEFAULT 'ACTIVO'
        CHECK (estado_operativo IN (
            'ACTIVO',
            'ENVIADO_TOTAL',
            'ENVIADO_PARCIAL',
            'CANCELADO'
        )),
    observaciones TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (import_job_id) REFERENCES import_jobs(id),
    FOREIGN KEY (pedido_origen_id) REFERENCES pedidos(id) ON DELETE SET NULL,
    CHECK (id_pedido <> '')
);

CREATE TABLE IF NOT EXISTS pedido_lineas (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pedido_id INTEGER NOT NULL,
    line_no INTEGER,
    sku TEXT,
    descripcion TEXT,
    producto TEXT,
    color TEXT,
    cantidad_pedida REAL NOT NULL DEFAULT 0
        CHECK (cantidad_pedida >= 0),
    cantidad_asignada REAL NOT NULL DEFAULT 0
        CHECK (cantidad_asignada >= 0),
    cantidad_faltante REAL NOT NULL DEFAULT 0
        CHECK (cantidad_faltante >= 0),
    cantidad_enviada REAL NOT NULL DEFAULT 0
        CHECK (cantidad_enviada >= 0),
    fecha_captura TEXT,
    fecha_produccion TEXT,
    fecha_logistica TEXT,
    estatus_venta TEXT,
    prioridad_manual INTEGER,
    observaciones TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
    UNIQUE (pedido_id, line_no),
    CHECK (cantidad_asignada <= cantidad_pedida),
    CHECK (cantidad_enviada <= cantidad_pedida)
);

-- =========================================================
-- CIERRES DE PEDIDOS
-- Se guarda evidencia operativa y snapshot de líneas para no
-- perder historia en la pestaña de enviados.
-- =========================================================
CREATE TABLE IF NOT EXISTS pedido_cierres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pedido_id INTEGER NOT NULL UNIQUE,
    tipo_cierre TEXT NOT NULL
        CHECK (tipo_cierre IN ('TOTAL', 'PARCIAL')),
    fecha_envio TEXT NOT NULL,
    usuario TEXT,
    observacion TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pedido_linea_cierres (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pedido_cierre_id INTEGER NOT NULL,
    pedido_linea_id INTEGER NOT NULL,
    sku TEXT,
    descripcion TEXT,
    producto TEXT,
    color TEXT,
    fecha_produccion TEXT,
    cantidad_pedida_original REAL NOT NULL DEFAULT 0
        CHECK (cantidad_pedida_original >= 0),
    cantidad_asignada_al_cierre REAL NOT NULL DEFAULT 0
        CHECK (cantidad_asignada_al_cierre >= 0),
    cantidad_enviada REAL NOT NULL DEFAULT 0
        CHECK (cantidad_enviada >= 0),
    cantidad_no_enviada REAL NOT NULL DEFAULT 0
        CHECK (cantidad_no_enviada >= 0),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pedido_cierre_id) REFERENCES pedido_cierres(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_linea_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE
);

-- =========================================================
-- AUDITORÍA
-- =========================================================
CREATE TABLE IF NOT EXISTS auditoria_cambios (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tabla TEXT NOT NULL,
    registro_id TEXT NOT NULL,
    campo TEXT NOT NULL,
    valor_anterior TEXT,
    valor_nuevo TEXT,
    usuario TEXT,
    motivo TEXT,
    fecha_cambio TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- CORTES DE ENTRADAS DE PRODUCCIÓN
-- =========================================================
CREATE TABLE IF NOT EXISTS cortes_entrada (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_job_id INTEGER NOT NULL,
    fecha_corte TEXT NOT NULL,
    hora_corte_texto TEXT,
    hora_corte_normalizada TEXT NOT NULL,
    total_filas INTEGER NOT NULL DEFAULT 0,
    total_piezas REAL NOT NULL DEFAULT 0,
    estado_corte TEXT NOT NULL DEFAULT 'PENDIENTE'
        CHECK (estado_corte IN (
            'PENDIENTE',
            'EN_REVISION',
            'PROCESADO',
            'REPROCESADO',
            'ANULADO'
        )),
    procesado_por TEXT,
    procesado_at TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (import_job_id) REFERENCES import_jobs(id) ON DELETE CASCADE,
    UNIQUE (fecha_corte, hora_corte_normalizada)
);

CREATE TABLE IF NOT EXISTS entradas_staging (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corte_id INTEGER NOT NULL,
    row_num INTEGER NOT NULL,
    sku TEXT,
    descripcion TEXT,
    cantidad REAL NOT NULL DEFAULT 0
        CHECK (cantidad >= 0),
    cajas REAL,
    fecha TEXT,
    hora TEXT,
    texto_original_destino TEXT,
    sugerencia_destino_texto TEXT,
    tipo_sugerencia TEXT NOT NULL DEFAULT 'SIN_DATO'
        CHECK (tipo_sugerencia IN (
            'PEDIDO',
            'STOCK',
            'AMBIGUA',
            'SIN_DATO'
        )),
    pedido_sugerido_texto TEXT,
    estado_revision TEXT NOT NULL DEFAULT 'PENDIENTE'
        CHECK (estado_revision IN (
            'PENDIENTE',
            'RESUELTA',
            'EN_REVISION',
            'IGNORADA'
        )),
    row_hash TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (corte_id) REFERENCES cortes_entrada(id) ON DELETE CASCADE,
    UNIQUE (corte_id, row_num)
);

-- =========================================================
-- DECISIONES SOBRE ENTRADAS
-- Permite dividir una entrada entre varios destinos.
-- =========================================================
CREATE TABLE IF NOT EXISTS entrada_decisiones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entrada_staging_id INTEGER NOT NULL,
    decision_type TEXT NOT NULL
        CHECK (decision_type IN (
            'STOCK_LIBRE',
            'ASIGNACION_PEDIDO',
            'PENDIENTE',
            'IGNORADA'
        )),
    pedido_id INTEGER,
    pedido_linea_id INTEGER,
    sku TEXT NOT NULL,
    cantidad REAL NOT NULL DEFAULT 0
        CHECK (cantidad >= 0),
    observacion TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (entrada_staging_id) REFERENCES entradas_staging(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE SET NULL,
    FOREIGN KEY (pedido_linea_id) REFERENCES pedido_lineas(id) ON DELETE SET NULL
);

-- =========================================================
-- INVENTARIO OPERATIVO MÍNIMO (RESUMEN)
-- =========================================================
CREATE TABLE IF NOT EXISTS inventario_sku (
    sku TEXT PRIMARY KEY,
    descripcion TEXT,
    total_recibido REAL NOT NULL DEFAULT 0,
    stock_libre REAL NOT NULL DEFAULT 0,
    stock_asignado REAL NOT NULL DEFAULT 0,
    total_enviado REAL NOT NULL DEFAULT 0,
    fecha_actualizacion TEXT,
    import_job_id INTEGER,
    origen_carga TEXT NOT NULL DEFAULT 'DESCONOCIDO',
    stock_base_libre REAL NOT NULL DEFAULT 0,
    stock_base_asignado REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (total_recibido >= 0),
    CHECK (stock_libre >= 0),
    CHECK (stock_asignado >= 0),
    CHECK (total_enviado >= 0)
);

-- =========================================================
-- MOVIMIENTOS DE INVENTARIO
-- =========================================================
CREATE TABLE IF NOT EXISTS inventario_movimientos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL,
    movimiento_tipo TEXT NOT NULL
        CHECK (movimiento_tipo IN (
            'ENTRADA_A_STOCK',
            'ASIGNACION_DESDE_STOCK',
            'DESASIGNACION_A_STOCK',
            'SALIDA_DESDE_ASIGNADO',
            'SALIDA_DESDE_STOCK',
            'AJUSTE_MANUAL',
            'CARGA_INICIAL',
            'LIBERACION_ASIGNADO_POR_CIERRE'
        )),
    afecta_total REAL NOT NULL DEFAULT 0,
    afecta_stock_libre REAL NOT NULL DEFAULT 0,
    afecta_stock_asignado REAL NOT NULL DEFAULT 0,
    referencia_tipo TEXT
        CHECK (referencia_tipo IN (
            'CORTE_ENTRADA',
            'ENTRADA_STAGING',
            'ENTRADA_DECISION',
            'ASIGNACION',
            'PEDIDO_CIERRE',
            'PEDIDO',
            'AJUSTE_MANUAL',
            'CARGA_INICIAL'
        )),
    referencia_id INTEGER,
    observacion TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =========================================================
-- ASIGNACIONES REALES A LÍNEAS DE PEDIDO
-- =========================================================
CREATE TABLE IF NOT EXISTS asignaciones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pedido_id INTEGER NOT NULL,
    pedido_linea_id INTEGER NOT NULL,
    sku TEXT NOT NULL,
    cantidad REAL NOT NULL DEFAULT 0
        CHECK (cantidad >= 0),
    fuente_asignacion TEXT NOT NULL
        CHECK (fuente_asignacion IN (
            'ENTRADA_PRODUCCION',
            'STOCK_LIBRE',
            'CARGA_INICIAL',
            'AJUSTE_MANUAL'
        )),
    entrada_staging_id INTEGER,
    decision_id INTEGER,
    estado TEXT NOT NULL DEFAULT 'ACTIVA'
        CHECK (estado IN (
            'ACTIVA',
            'CANCELADA',
            'CONSUMIDA'
        )),
    observacion TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pedido_id) REFERENCES pedidos(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_linea_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE,
    FOREIGN KEY (entrada_staging_id) REFERENCES entradas_staging(id) ON DELETE SET NULL,
    FOREIGN KEY (decision_id) REFERENCES entrada_decisiones(id) ON DELETE SET NULL
);

-- =========================================================
-- HISTORIAL DE REASIGNACIONES DE RESERVA
-- =========================================================
CREATE TABLE IF NOT EXISTS reasignaciones_reserva (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fecha TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sku TEXT NOT NULL,
    pedido_origen_id INTEGER NOT NULL,
    pedido_destino_id INTEGER NOT NULL,
    pedido_linea_origen_id INTEGER NOT NULL,
    pedido_linea_destino_id INTEGER NOT NULL,
    cantidad REAL NOT NULL DEFAULT 0
        CHECK (cantidad > 0),
    motivo TEXT,
    observacion TEXT,
    usuario TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pedido_origen_id) REFERENCES pedidos(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_destino_id) REFERENCES pedidos(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_linea_origen_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE,
    FOREIGN KEY (pedido_linea_destino_id) REFERENCES pedido_lineas(id) ON DELETE CASCADE
);


-- =========================================================
-- CORTES FÍSICOS DE INVENTARIO
-- =========================================================
CREATE TABLE IF NOT EXISTS inventario_cortes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    import_job_id INTEGER,
    tipo_corte TEXT NOT NULL DEFAULT 'CONTEO_PARCIAL'
        CHECK (tipo_corte IN ('CONTEO_PARCIAL', 'CONTEO_TOTAL')),
    archivo_nombre TEXT,
    hoja_nombre TEXT,
    fecha_corte TEXT NOT NULL,
    total_filas INTEGER NOT NULL DEFAULT 0,
    filas_aplicadas INTEGER NOT NULL DEFAULT 0,
    filas_revision INTEGER NOT NULL DEFAULT 0,
    diferencia_total REAL NOT NULL DEFAULT 0,
    estado TEXT NOT NULL DEFAULT 'APLICADO'
        CHECK (estado IN ('PREVIEW', 'APLICADO', 'APLICADO_CON_REVISION', 'ANULADO')),
    usuario TEXT,
    observacion TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (import_job_id) REFERENCES import_jobs(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS inventario_corte_detalle (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    corte_id INTEGER NOT NULL,
    row_num INTEGER,
    sku TEXT,
    descripcion TEXT,
    stock_sistema REAL NOT NULL DEFAULT 0,
    stock_asignado REAL NOT NULL DEFAULT 0,
    stock_contado REAL NOT NULL DEFAULT 0,
    diferencia REAL NOT NULL DEFAULT 0,
    fecha_actualizacion TEXT,
    estado_detalle TEXT NOT NULL DEFAULT 'APLICADO'
        CHECK (estado_detalle IN ('APLICADO', 'REVISION')),
    motivo_revision TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (corte_id) REFERENCES inventario_cortes(id) ON DELETE CASCADE
);

-- =========================================================
-- ÍNDICES
-- =========================================================
CREATE INDEX IF NOT EXISTS idx_pedidos_estado_operativo
    ON pedidos (estado_operativo);

CREATE INDEX IF NOT EXISTS idx_pedidos_cliente
    ON pedidos (cliente);

CREATE INDEX IF NOT EXISTS idx_pedidos_fecha_captura
    ON pedidos (fecha_captura);

CREATE INDEX IF NOT EXISTS idx_pedidos_fecha_produccion
    ON pedidos (fecha_produccion);

CREATE INDEX IF NOT EXISTS idx_pedidos_fecha_logistica
    ON pedidos (fecha_logistica);

CREATE INDEX IF NOT EXISTS idx_pedidos_origen
    ON pedidos (pedido_origen_id);

CREATE INDEX IF NOT EXISTS idx_lineas_pedido_id
    ON pedido_lineas (pedido_id);

CREATE INDEX IF NOT EXISTS idx_lineas_sku
    ON pedido_lineas (sku);

CREATE INDEX IF NOT EXISTS idx_lineas_producto
    ON pedido_lineas (producto);

CREATE INDEX IF NOT EXISTS idx_lineas_fecha_produccion
    ON pedido_lineas (fecha_produccion);

CREATE INDEX IF NOT EXISTS idx_lineas_fecha_logistica
    ON pedido_lineas (fecha_logistica);

CREATE INDEX IF NOT EXISTS idx_lineas_faltante
    ON pedido_lineas (cantidad_faltante);

CREATE INDEX IF NOT EXISTS idx_cortes_fecha_hora
    ON cortes_entrada (fecha_corte, hora_corte_normalizada);

CREATE INDEX IF NOT EXISTS idx_staging_corte
    ON entradas_staging (corte_id);

CREATE INDEX IF NOT EXISTS idx_staging_sku
    ON entradas_staging (sku);

CREATE INDEX IF NOT EXISTS idx_decisiones_entrada
    ON entrada_decisiones (entrada_staging_id);

CREATE INDEX IF NOT EXISTS idx_decisiones_linea
    ON entrada_decisiones (pedido_linea_id);

CREATE INDEX IF NOT EXISTS idx_asignaciones_pedido_linea
    ON asignaciones (pedido_linea_id);

CREATE INDEX IF NOT EXISTS idx_asignaciones_sku
    ON asignaciones (sku);

CREATE INDEX IF NOT EXISTS idx_inventario_movimientos_sku
    ON inventario_movimientos (sku);

CREATE INDEX IF NOT EXISTS idx_inventario_cortes_fecha
    ON inventario_cortes (fecha_corte, created_at);

CREATE INDEX IF NOT EXISTS idx_inventario_corte_detalle_corte
    ON inventario_corte_detalle (corte_id, estado_detalle);
CREATE INDEX IF NOT EXISTS idx_reasignaciones_reserva_sku
    ON reasignaciones_reserva (sku, created_at DESC);


-- =========================================================
-- TRIGGERS updated_at
-- =========================================================
CREATE TRIGGER IF NOT EXISTS trg_usuarios_updated_at
AFTER UPDATE ON usuarios
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE usuarios
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_pedidos_updated_at
AFTER UPDATE ON pedidos
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE pedidos
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_pedido_lineas_updated_at
AFTER UPDATE ON pedido_lineas
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE pedido_lineas
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_entradas_staging_updated_at
AFTER UPDATE ON entradas_staging
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE entradas_staging
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_asignaciones_updated_at
AFTER UPDATE ON asignaciones
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE asignaciones
    SET updated_at = CURRENT_TIMESTAMP
    WHERE id = NEW.id;
END;

CREATE TRIGGER IF NOT EXISTS trg_inventario_sku_updated_at
AFTER UPDATE ON inventario_sku
FOR EACH ROW
WHEN NEW.updated_at = OLD.updated_at
BEGIN
    UPDATE inventario_sku
    SET updated_at = CURRENT_TIMESTAMP
    WHERE sku = NEW.sku;
END;


-- =========================================================
-- BOOTSTRAP INICIAL
-- Registro de corridas especiales de inicialización del MVP.
-- =========================================================
CREATE TABLE IF NOT EXISTS bootstrap_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL DEFAULT 'PENDIENTE',
    inventory_file_name TEXT,
    pedidos_file_name TEXT,
    entradas_file_name TEXT,
    inventory_file_hash TEXT,
    pedidos_file_hash TEXT,
    entradas_file_hash TEXT,
    summary_json TEXT,
    notes TEXT,
    created_by TEXT,
    started_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TEXT
);

COMMIT;
