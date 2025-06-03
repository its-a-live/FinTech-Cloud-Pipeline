import uuid
from typing import Any, Dict, List
from .dds_model import *
from lib.pg import PgConnect
from psycopg.rows import class_row
import hashlib


def input_to_uuid(*args):
    input_string = ''.join(args)
    hash_object = hashlib.md5(input_string.encode())
    hash_hex = hash_object.hexdigest()

    return uuid.UUID(hash_hex)



class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def h_user_insert(self,
                      user: UserObj,
                      load_src: str
                      ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user (h_user_pk, user_id, load_dt, load_src)
                        VALUES (%(h_user_pk)s, %(user_id)s, NOW(), %(load_src)s)
                        ON CONFLICT(user_id) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_src': load_src
                    }
                )

    def h_product_insert(self,
                         product: ProductObj,
                         load_src: str
                         ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product (h_product_pk, product_id, load_dt, load_src)
                        VALUES (%(h_product_pk)s, %(product_id)s, NOW(), %(load_src)s)
                        ON CONFLICT(product_id) DO NOTHING;
                    """,
                    {
                        'h_product_pk': product.h_product_pk,
                        'product_id': product.product_id,
                        'load_src': load_src
                    }
                )
                conn.commit()

    def h_category_insert(self,
                          category: CategoryObj,
                          load_src: str
                          ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category (h_category_pk, category_name, load_dt, load_src)
                        VALUES (%(h_category_pk)s, %(category_name)s, NOW(), %(load_src)s)
                        ON CONFLICT(category_name) DO NOTHING;
                    """,
                    {
                        'h_category_pk': category.h_category_pk,
                        'category_name': category.category_name,
                        'load_src': load_src
                    }
                )

    def h_restaurant_insert(self,
                            restaurant: RestaurantObj,
                            load_src: str
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant (h_restaurant_pk, restaurant_id, load_dt, load_src)
                        VALUES (%(h_restaurant_pk)s, %(restaurant_id)s, NOW(), %(load_src)s)
                        ON CONFLICT(restaurant_id) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_src': load_src
                    }
                )

    def h_order_insert(self,
                       order: OrderObj,
                       load_src: str
                       ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order (h_order_pk, order_id, order_dt, load_dt, load_src)
                        VALUES (%(h_order_pk)s, %(order_id)s, %(order_dt)s, NOW(), %(load_src)s)
                        ON CONFLICT(order_id) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_src': load_src
                    }
                )
                conn.commit()

    #insert links

    def l_order_product_insert(self,
                               order: OrderObj,
                               product: ProductObj,
                               load_src: str
                               ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product (hk_order_product_pk, h_order_pk, h_product_pk, load_dt, load_src)
                        select %(hk_order_product_pk)s, h_order_pk, h_product_pk, NOW(), %(load_src)s
						from dds.h_order  cross join dds.h_product  
						where order_id = %(order_id)s and product_id = %(product_id)s
                        ON CONFLICT(h_order_pk, h_product_pk) DO NOTHING;
                    """,
                    {'hk_order_product_pk': input_to_uuid(str(order.order_id), product.product_id),
                     'order_id': order.order_id,
                     'product_id': product.product_id,
                     'load_src': load_src
                     }
                )

    def l_product_restaurant_insert(self,
                                    restaurant: RestaurantObj,
                                    product: ProductObj,
                                    load_src: str
                                    ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant
                        (hk_product_restaurant_pk, h_restaurant_pk, h_product_pk, load_dt, load_src)
                        select %(hk_product_restaurant_pk)s, h_restaurant_pk, h_product_pk, NOW(), %(load_src)s
                        from dds.h_product cross join dds.h_restaurant  
                        where restaurant_id = %(restaurant_id)s 
                        and product_id = %(product_id)s
                        ON CONFLICT(h_restaurant_pk, h_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': input_to_uuid(restaurant.restaurant_id, product.product_id),
                        'restaurant_id': restaurant.restaurant_id,
                        'product_id': product.product_id,
                        'load_src': load_src
                    }
                )

    def l_product_category_insert(self,
                                  product: ProductObj,
                                  load_src: str
                                  ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category
                        (hk_product_category_pk, h_category_pk, h_product_pk, load_dt, load_src)
                        SELECT %(hk_product_category_pk)s, h_category_pk, h_product_pk, NOW(), %(load_src)s
                        from dds.h_product cross join dds.h_category  
                        where category_name  = %(category_name)s 
                        and product_id = %(product_id)s
                        ON CONFLICT(h_category_pk, h_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': input_to_uuid(product.category_name, product.product_id),
                        'category_name': product.category_name,
                        'product_id': product.product_id,
                        'load_src': load_src
                    }
                )

    def l_order_user_insert(self,
                            user: UserObj,
                            order: OrderObj,
                            load_src: str
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user
                        (hk_order_user_pk, h_user_pk, h_order_pk, load_dt, load_src)
                        SELECT %(hk_order_user_pk)s, h_user_pk, h_order_pk, NOW(), %(load_src)s
                        from dds.h_user cross join dds.h_order  
                        where user_id  = %(user_id)s 
                        and order_id = %(order_id)s
                        ON CONFLICT(h_user_pk, h_order_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': input_to_uuid(user.user_id, str(order.order_id)),
                        'user_id': user.user_id,
                        'order_id': order.order_id,
                        'load_src': load_src
                    }
                )

    #insert satelits
    def s_user_names_insert(self,
                            user: UserObj,
                            load_src: str
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names
                        (h_user_pk, username, userlogin, hk_user_names_hashdiff, load_dt, load_src)
                        SELECT h_user_pk, %(user_name)s, %(user_login)s, %(hk_user_names_hashdiff)s, NOW(), %(load_src)s
                        FROM dds.h_user
                        where user_id = %(user_id)s
                        ON CONFLICT(h_user_pk) DO NOTHING;
                    """,
                    {
                        'user_id': user.user_id,
                        'user_name': user.user_name,
                        'user_login': user.user_login,
                        'hk_user_names_hashdiff': input_to_uuid(user.user_id),
                        'load_src': load_src
                    }
                )

    def s_product_names_insert(self,
                               product: ProductObj,
                               load_src: str
                               ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names
                        (h_product_pk, "name", hk_product_names_hashdiff, load_dt, load_src)
                        SELECT h_product_pk, %(product_name)s, %(hk_product_names_hashdiff)s, NOW(), %(load_src)s
                        FROM dds.h_product
                        where product_id = %(product_id)s
                        ON CONFLICT(h_product_pk) DO NOTHING;
                    """,
                    {
                        'product_id': product.product_id,
                        'product_name': product.product_name,
                        'hk_product_names_hashdiff': input_to_uuid(product.product_id),
                        'load_src': load_src
                    }
                )

    def s_restaurant_names_insert(self,
                                  restaurant: RestaurantObj,
                                  load_src: str
                                  ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names
                        (h_restaurant_pk, "name", hk_restaurant_names_hashdiff, load_dt, load_src)
                        SELECT h_restaurant_pk, %(restaurant_name)s, %(hk_restaurant_names_hashdiff)s, NOW(), %(load_src)s
                        FROM dds.h_restaurant
                        where restaurant_id = %(restaurant_id)s
                        ON CONFLICT(h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'restaurant_id': restaurant.restaurant_id,
                        'restaurant_name': restaurant.restaurant_name,
                        'hk_restaurant_names_hashdiff': input_to_uuid(restaurant.restaurant_id),
                        'load_src': load_src
                    }
                )

    def s_order_cost_insert(self,
                            order: OrderObj,
                            load_src: str
                            ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost
                        (h_order_pk, "cost", payment, hk_order_cost_hashdiff, load_dt, load_src)
                        SELECT h_order_pk, %(cost)s, %(payment)s, %(hk_order_cost_hashdiff)s, NOW(), %(load_src)s
                        FROM dds.h_order
                        where order_id = %(order_id)s
                        ON CONFLICT(h_order_pk) DO NOTHING;
                    """,
                    {
                        'order_id': order.order_id,
                        'cost': order.cost,
                        'payment': order.payment,
                        'hk_order_cost_hashdiff': input_to_uuid(str(order.order_id)),
                        'load_src': load_src
                    }
                )

    def s_order_status_insert(self,
                              order: OrderObj,
                              load_src: str
                              ) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status
                        (h_order_pk, status, hk_order_status_hashdiff, load_dt, load_src)
                        SELECT h_order_pk, %(status)s, %(hk_order_cost_hashdiff)s, NOW(), %(load_src)s
                        FROM dds.h_order
                        where order_id = %(order_id)s
                        ON CONFLICT(h_order_pk) DO NOTHING;
                    """,
                    {
                        'order_id': order.order_id,
                        'status': order.status,
                        'hk_order_cost_hashdiff': input_to_uuid(str(order.order_id)),
                        'load_src': load_src
                    }
                )


    def output_message(self, product_ids: tuple, user: UserObj) -> List[OutputMsgObj]:
        with self._db.connection() as conn:
            with conn.cursor(row_factory=class_row(OutputMsgObj)) as cur:
                query = """
                SELECT cast(hu.h_user_pk as varchar) as user_id, cast(pn.h_product_pk as varchar) as product_id,
                 pn."name" as product_name, COUNT(DISTINCT o.h_order_pk) as order_cnt
                FROM dds.h_user hu
                JOIN dds.l_order_user ou ON ou.h_user_pk = hu.h_user_pk
                JOIN dds.h_order o ON o.h_order_pk = ou.h_order_pk
                JOIN dds.l_order_product op ON op.h_order_pk = o.h_order_pk
                JOIN dds.h_product p ON op.h_product_pk = p.h_product_pk
                JOIN dds.s_product_names pn ON pn.h_product_pk = op.h_product_pk
                JOIN dds.s_order_status os on o.h_order_pk = os.h_order_pk
                WHERE hu.user_id = %(user_id)s AND p.product_id = ANY(%(product_ids)s)
                AND os.status = 'CLOSED'
                GROUP BY hu.h_user_pk, pn.h_product_pk, pn."name"
                """
                cur.execute(query, {"user_id": user.user_id, "product_ids": '{' + ','.join(product_ids) + '}'})
                objs = cur.fetchall()

            return objs
