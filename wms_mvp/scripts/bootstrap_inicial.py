from __future__ import annotations

import argparse
from pathlib import Path

from wms_mvp.core.services.bootstrap_service import apply_bootstrap_inicial


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap inicial del MVP")
    parser.add_argument("--inventario", required=True, help="Ruta al archivo de inventario base")
    parser.add_argument("--pedidos", required=True, help="Ruta al archivo de pedidos iniciales")
    parser.add_argument("--entradas", required=True, help="Ruta al archivo de entradas históricas")
    parser.add_argument("--usuario", default="Bootstrap", help="Usuario que ejecuta la corrida")
    parser.add_argument("--sin-reset", action="store_true", help="No borrar datos existentes antes de ejecutar")
    args = parser.parse_args()

    inv_path = Path(args.inventario)
    ped_path = Path(args.pedidos)
    ent_path = Path(args.entradas)

    result = apply_bootstrap_inicial(
        inventory_file_name=inv_path.name,
        inventory_file_bytes=inv_path.read_bytes(),
        pedidos_file_name=ped_path.name,
        pedidos_file_bytes=ped_path.read_bytes(),
        entradas_file_name=ent_path.name,
        entradas_file_bytes=ent_path.read_bytes(),
        created_by=args.usuario,
        reset_existing=not args.sin_reset,
    )
    print("Bootstrap completado")
    print(result)


if __name__ == "__main__":
    main()
