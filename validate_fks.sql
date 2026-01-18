-- FK VALIDATE Script
-- 이 스크립트는 트래픽이 적은 시간대에 실행하세요.
-- VALIDATE는 전체 테이블을 스캔하므로 시간이 오래 걸릴 수 있습니다.
-- Total: 136 FK constraints

-- Progress tracking:
-- \timing on

-- [1/136] Validating ext_option.ext_option_cur_id_fkey
ALTER TABLE ext_option VALIDATE CONSTRAINT "ext_option_cur_id_fkey";

-- [2/136] Validating ext_option.ext_option_opts_id_fkey
ALTER TABLE ext_option VALIDATE CONSTRAINT "ext_option_opts_id_fkey";

-- [3/136] Validating cur_menu_section_item.cur_menu_section_item_cur_menu_item_id_fkey
ALTER TABLE cur_menu_section_item VALIDATE CONSTRAINT "cur_menu_section_item_cur_menu_item_id_fkey";

-- [4/136] Validating cur_menu_section_item.cur_menu_section_item_cur_menu_section_id_fkey
ALTER TABLE cur_menu_section_item VALIDATE CONSTRAINT "cur_menu_section_item_cur_menu_section_id_fkey";

-- [5/136] Validating ext_menu_touch_key_item.ext_menu_touch_key_item_cur_menu_item_id_fkey
ALTER TABLE ext_menu_touch_key_item VALIDATE CONSTRAINT "ext_menu_touch_key_item_cur_menu_item_id_fkey";

-- [6/136] Validating ext_menu_touch_key_item.ext_menu_touch_key_item_ext_menu_id_class_key_fkey
ALTER TABLE ext_menu_touch_key_item VALIDATE CONSTRAINT "ext_menu_touch_key_item_ext_menu_id_class_key_fkey";

-- [7/136] Validating cur_menu_item.cur_menu_item_cur_menu_id_fkey
ALTER TABLE cur_menu_item VALIDATE CONSTRAINT "cur_menu_item_cur_menu_id_fkey";

-- [8/136] Validating cur_menu_item.cur_menu_item_menu_item_id_fkey
ALTER TABLE cur_menu_item VALIDATE CONSTRAINT "cur_menu_item_menu_item_id_fkey";

-- [9/136] Validating ext_option_set_schema_lang.ext_option_set_schema_lang_ext_menu_id_fkey
ALTER TABLE ext_option_set_schema_lang VALIDATE CONSTRAINT "ext_option_set_schema_lang_ext_menu_id_fkey";

-- [10/136] Validating cur_option_set_schema.cur_option_set_schema_cur_menu_id_fkey
ALTER TABLE cur_option_set_schema VALIDATE CONSTRAINT "cur_option_set_schema_cur_menu_id_fkey";
-- Progress: 10/136 completed

-- [11/136] Validating cur_option_set_schema.cur_option_set_schema_ext_id_fkey
ALTER TABLE cur_option_set_schema VALIDATE CONSTRAINT "cur_option_set_schema_ext_id_fkey";

-- [12/136] Validating cur_option_set_schema.cur_option_set_schema_opts_set_schema_id_fkey
ALTER TABLE cur_option_set_schema VALIDATE CONSTRAINT "cur_option_set_schema_opts_set_schema_id_fkey";

-- [13/136] Validating ext_menu.ext_menu_ext_shop_id_fkey
ALTER TABLE ext_menu VALIDATE CONSTRAINT "ext_menu_ext_shop_id_fkey";

-- [14/136] Validating ext_menu.ext_menu_menu_id_fkey
ALTER TABLE ext_menu VALIDATE CONSTRAINT "ext_menu_menu_id_fkey";

-- [15/136] Validating menu_item_options.menu_item_options_menu_item_id_fkey
ALTER TABLE menu_item_options VALIDATE CONSTRAINT "menu_item_options_menu_item_id_fkey";

-- [16/136] Validating ext_in_box.ext_in_box_ext_menu_id_fkey
ALTER TABLE ext_in_box VALIDATE CONSTRAINT "ext_in_box_ext_menu_id_fkey";

-- [17/136] Validating message_attachment.message_attachment_message_id_fkey
ALTER TABLE message_attachment VALIDATE CONSTRAINT "message_attachment_message_id_fkey";

-- [18/136] Validating thread_message.thread_message_parent_message_id_fkey
ALTER TABLE thread_message VALIDATE CONSTRAINT "thread_message_parent_message_id_fkey";

-- [19/136] Validating thread_message.thread_message_thread_id_fkey
ALTER TABLE thread_message VALIDATE CONSTRAINT "thread_message_thread_id_fkey";

-- [20/136] Validating ext_menu_section_item.ext_menu_section_item_ext_menu_item_id_fkey
ALTER TABLE ext_menu_section_item VALIDATE CONSTRAINT "ext_menu_section_item_ext_menu_item_id_fkey";
-- Progress: 20/136 completed

-- [21/136] Validating ext_menu_section_item.ext_menu_section_item_ext_menu_section_id_fkey
ALTER TABLE ext_menu_section_item VALIDATE CONSTRAINT "ext_menu_section_item_ext_menu_section_id_fkey";

-- [22/136] Validating ext_option_set_schema.ext_option_set_schema_cur_id_fkey
ALTER TABLE ext_option_set_schema VALIDATE CONSTRAINT "ext_option_set_schema_cur_id_fkey";

-- [23/136] Validating ext_option_set_schema.ext_option_set_schema_opts_set_schema_id_fkey
ALTER TABLE ext_option_set_schema VALIDATE CONSTRAINT "ext_option_set_schema_opts_set_schema_id_fkey";

-- [24/136] Validating owner_images.public_owner_images_owner_id_fkey
ALTER TABLE owner_images VALIDATE CONSTRAINT "public_owner_images_owner_id_fkey";

-- [25/136] Validating menu_items.menu_items_menu_id_fkey
ALTER TABLE menu_items VALIDATE CONSTRAINT "menu_items_menu_id_fkey";

-- [26/136] Validating menu_items.menu_items_menu_section_id_fkey
ALTER TABLE menu_items VALIDATE CONSTRAINT "menu_items_menu_section_id_fkey";

-- [27/136] Validating bots.bots_owner_id_fkey
ALTER TABLE bots VALIDATE CONSTRAINT "bots_owner_id_fkey";

-- [28/136] Validating menu_images.public_menu_images_menu_id_fkey
ALTER TABLE menu_images VALIDATE CONSTRAINT "public_menu_images_menu_id_fkey";

-- [29/136] Validating menu_images.public_menu_images_owner_id_fkey
ALTER TABLE menu_images VALIDATE CONSTRAINT "public_menu_images_owner_id_fkey";

-- [30/136] Validating ext_menu_item_lang.ext_menu_item_lang_ext_menu_id_fkey
ALTER TABLE ext_menu_item_lang VALIDATE CONSTRAINT "ext_menu_item_lang_ext_menu_id_fkey";
-- Progress: 30/136 completed

-- [31/136] Validating menu_order_settings.menu_order_settings_menu_id_fkey
ALTER TABLE menu_order_settings VALIDATE CONSTRAINT "menu_order_settings_menu_id_fkey";

-- [32/136] Validating menu_section_items.menu_section_items_menu_id_fkey
ALTER TABLE menu_section_items VALIDATE CONSTRAINT "menu_section_items_menu_id_fkey";

-- [33/136] Validating menu_section_items.menu_section_items_menu_item_id_fkey
ALTER TABLE menu_section_items VALIDATE CONSTRAINT "menu_section_items_menu_item_id_fkey";

-- [34/136] Validating menu_section_items.menu_section_items_section_id_fkey
ALTER TABLE menu_section_items VALIDATE CONSTRAINT "menu_section_items_section_id_fkey";

-- [35/136] Validating chat_history.public_chat_history_owner_id_fkey
ALTER TABLE chat_history VALIDATE CONSTRAINT "public_chat_history_owner_id_fkey";

-- [36/136] Validating image_to_text.public_image_to_text_image_id_fkey
ALTER TABLE image_to_text VALIDATE CONSTRAINT "public_image_to_text_image_id_fkey";

-- [37/136] Validating image_to_text.public_image_to_text_owner_id_fkey
ALTER TABLE image_to_text VALIDATE CONSTRAINT "public_image_to_text_owner_id_fkey";

-- [38/136] Validating cur_menu.cur_menu_ext_menu_id_fkey
ALTER TABLE cur_menu VALIDATE CONSTRAINT "cur_menu_ext_menu_id_fkey";

-- [39/136] Validating cur_menu.cur_menu_menu_id_fkey
ALTER TABLE cur_menu VALIDATE CONSTRAINT "cur_menu_menu_id_fkey";

-- [40/136] Validating menu_sections.menu_sections_menu_id_fkey
ALTER TABLE menu_sections VALIDATE CONSTRAINT "menu_sections_menu_id_fkey";
-- Progress: 40/136 completed

-- [41/136] Validating ext_menu_section_lang.ext_menu_section_lang_ext_menu_id_fkey
ALTER TABLE ext_menu_section_lang VALIDATE CONSTRAINT "ext_menu_section_lang_ext_menu_id_fkey";

-- [42/136] Validating ext_menu_item_desc.ext_menu_item_desc_ext_menu_id_fkey
ALTER TABLE ext_menu_item_desc VALIDATE CONSTRAINT "ext_menu_item_desc_ext_menu_id_fkey";

-- [43/136] Validating cur_option.cur_option_cur_option_schema_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_cur_option_schema_id_fkey";

-- [44/136] Validating cur_option.cur_option_cur_option_set_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_cur_option_set_id_fkey";

-- [45/136] Validating cur_option.cur_option_ext_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_ext_id_fkey";

-- [46/136] Validating cur_option.cur_option_opts_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_opts_id_fkey";

-- [47/136] Validating cur_option.cur_option_opts_schema_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_opts_schema_id_fkey";

-- [48/136] Validating cur_option.cur_option_opts_set_id_fkey
ALTER TABLE cur_option VALIDATE CONSTRAINT "cur_option_opts_set_id_fkey";

-- [49/136] Validating menu_item_opts_schema.menu_item_opts_schema_menu_id_fkey
ALTER TABLE menu_item_opts_schema VALIDATE CONSTRAINT "menu_item_opts_schema_menu_id_fkey";

-- [50/136] Validating menu_item_opts_schema.menu_item_opts_schema_set_id_fkey
ALTER TABLE menu_item_opts_schema VALIDATE CONSTRAINT "menu_item_opts_schema_set_id_fkey";
-- Progress: 50/136 completed

-- [51/136] Validating menu.menu_shop_id_fkey
ALTER TABLE menu VALIDATE CONSTRAINT "menu_shop_id_fkey";

-- [52/136] Validating shop_refs.public_shop_refs_shop_id_fkey
ALTER TABLE shop_refs VALIDATE CONSTRAINT "public_shop_refs_shop_id_fkey";

-- [53/136] Validating image_analysis.public_image_analysis_owner_id_fkey
ALTER TABLE image_analysis VALIDATE CONSTRAINT "public_image_analysis_owner_id_fkey";

-- [54/136] Validating image_analysis.public_image_analysis_src_image_id_fkey
ALTER TABLE image_analysis VALIDATE CONSTRAINT "public_image_analysis_src_image_id_fkey";

-- [55/136] Validating ext_snap_shot.ext_snap_shot_ext_menu_id_fkey
ALTER TABLE ext_snap_shot VALIDATE CONSTRAINT "ext_snap_shot_ext_menu_id_fkey";

-- [56/136] Validating menu_extra_charge_settings.menu_extra_charge_settings_menu_id_fkey
ALTER TABLE menu_extra_charge_settings VALIDATE CONSTRAINT "menu_extra_charge_settings_menu_id_fkey";

-- [57/136] Validating menu_extra_charge_settings.menu_extra_charge_settings_menu_item_id_fkey
ALTER TABLE menu_extra_charge_settings VALIDATE CONSTRAINT "menu_extra_charge_settings_menu_item_id_fkey";

-- [58/136] Validating cur_menu_item_lang.cur_menu_item_lang_cur_menu_item_id_fkey
ALTER TABLE cur_menu_item_lang VALIDATE CONSTRAINT "cur_menu_item_lang_cur_menu_item_id_fkey";

-- [59/136] Validating cur_menu_item_lang.cur_menu_item_lang_menu_item_id_fkey
ALTER TABLE cur_menu_item_lang VALIDATE CONSTRAINT "cur_menu_item_lang_menu_item_id_fkey";

-- [60/136] Validating cur_option_set_lang.cur_option_set_lang_cur_option_set_id_fkey
ALTER TABLE cur_option_set_lang VALIDATE CONSTRAINT "cur_option_set_lang_cur_option_set_id_fkey";
-- Progress: 60/136 completed

-- [61/136] Validating cur_menu_item_section_change.cur_menu_item_section_change_cur_menu_item_id_fkey
ALTER TABLE cur_menu_item_section_change VALIDATE CONSTRAINT "cur_menu_item_section_change_cur_menu_item_id_fkey";

-- [62/136] Validating cur_menu_item_section_change.cur_menu_item_section_change_cur_menu_section_id_fkey
ALTER TABLE cur_menu_item_section_change VALIDATE CONSTRAINT "cur_menu_item_section_change_cur_menu_section_id_fkey";

-- [63/136] Validating cur_menu_item_section_change.cur_menu_item_section_change_menu_section_id_fkey
ALTER TABLE cur_menu_item_section_change VALIDATE CONSTRAINT "cur_menu_item_section_change_menu_section_id_fkey";

-- [64/136] Validating pos_role_invitations.public_pos_role_invitations_menu_id_fkey
ALTER TABLE pos_role_invitations VALIDATE CONSTRAINT "public_pos_role_invitations_menu_id_fkey";

-- [65/136] Validating pos_role_invitations.public_pos_role_invitations_owner_id_fkey
ALTER TABLE pos_role_invitations VALIDATE CONSTRAINT "public_pos_role_invitations_owner_id_fkey";

-- [66/136] Validating pos_role_invitations.public_pos_role_invitations_pos_account_id_fkey
ALTER TABLE pos_role_invitations VALIDATE CONSTRAINT "public_pos_role_invitations_pos_account_id_fkey";

-- [67/136] Validating ext_menu_item.ext_menu_item_cur_id_fkey
ALTER TABLE ext_menu_item VALIDATE CONSTRAINT "ext_menu_item_cur_id_fkey";

-- [68/136] Validating ext_menu_item.ext_menu_item_ext_menu_id_fkey
ALTER TABLE ext_menu_item VALIDATE CONSTRAINT "ext_menu_item_ext_menu_id_fkey";

-- [69/136] Validating ext_menu_item.ext_menu_item_menu_item_id_fkey
ALTER TABLE ext_menu_item VALIDATE CONSTRAINT "ext_menu_item_menu_item_id_fkey";

-- [70/136] Validating ext_option_set.ext_option_set_cur_id_fkey
ALTER TABLE ext_option_set VALIDATE CONSTRAINT "ext_option_set_cur_id_fkey";
-- Progress: 70/136 completed

-- [71/136] Validating ext_option_set.ext_option_set_opts_set_id_fkey
ALTER TABLE ext_option_set VALIDATE CONSTRAINT "ext_option_set_opts_set_id_fkey";

-- [72/136] Validating cur_option_schema.cur_option_schema_cur_menu_id_fkey
ALTER TABLE cur_option_schema VALIDATE CONSTRAINT "cur_option_schema_cur_menu_id_fkey";

-- [73/136] Validating cur_option_schema.cur_option_schema_cur_option_set_schema_id_fkey
ALTER TABLE cur_option_schema VALIDATE CONSTRAINT "cur_option_schema_cur_option_set_schema_id_fkey";

-- [74/136] Validating cur_option_schema.cur_option_schema_ext_id_fkey
ALTER TABLE cur_option_schema VALIDATE CONSTRAINT "cur_option_schema_ext_id_fkey";

-- [75/136] Validating cur_option_schema.cur_option_schema_opts_schema_id_fkey
ALTER TABLE cur_option_schema VALIDATE CONSTRAINT "cur_option_schema_opts_schema_id_fkey";

-- [76/136] Validating cur_option_schema.cur_option_schema_opts_set_schema_id_fkey
ALTER TABLE cur_option_schema VALIDATE CONSTRAINT "cur_option_schema_opts_set_schema_id_fkey";

-- [77/136] Validating cur_menu_section_lang.cur_menu_section_lang_cur_menu_section_id_fkey
ALTER TABLE cur_menu_section_lang VALIDATE CONSTRAINT "cur_menu_section_lang_cur_menu_section_id_fkey";

-- [78/136] Validating priv_images.priv_images_deleted_by_owner_id_fkey
ALTER TABLE priv_images VALIDATE CONSTRAINT "priv_images_deleted_by_owner_id_fkey";

-- [79/136] Validating priv_images.priv_images_org_owner_id_fkey
ALTER TABLE priv_images VALIDATE CONSTRAINT "priv_images_org_owner_id_fkey";

-- [80/136] Validating priv_images.priv_images_owner_id_fkey
ALTER TABLE priv_images VALIDATE CONSTRAINT "priv_images_owner_id_fkey";
-- Progress: 80/136 completed

-- [81/136] Validating priv_images.priv_images_shop_id_fkey
ALTER TABLE priv_images VALIDATE CONSTRAINT "priv_images_shop_id_fkey";

-- [82/136] Validating cur_option_lang.cur_option_lang_cur_option_id_fkey
ALTER TABLE cur_option_lang VALIDATE CONSTRAINT "cur_option_lang_cur_option_id_fkey";

-- [83/136] Validating menu_item_images.menu_item_images_menu_id_fkey
ALTER TABLE menu_item_images VALIDATE CONSTRAINT "menu_item_images_menu_id_fkey";

-- [84/136] Validating menu_item_images.menu_item_images_menu_item_id_fkey
ALTER TABLE menu_item_images VALIDATE CONSTRAINT "menu_item_images_menu_item_id_fkey";

-- [85/136] Validating ext_menu_section.ext_menu_section_cur_id_fkey
ALTER TABLE ext_menu_section VALIDATE CONSTRAINT "ext_menu_section_cur_id_fkey";

-- [86/136] Validating ext_menu_section.ext_menu_section_ext_menu_id_fkey
ALTER TABLE ext_menu_section VALIDATE CONSTRAINT "ext_menu_section_ext_menu_id_fkey";

-- [87/136] Validating ext_menu_section.ext_menu_section_menu_section_id_fkey
ALTER TABLE ext_menu_section VALIDATE CONSTRAINT "ext_menu_section_menu_section_id_fkey";

-- [88/136] Validating roles.roles_invited_by_fkey
ALTER TABLE roles VALIDATE CONSTRAINT "roles_invited_by_fkey";

-- [89/136] Validating roles.roles_invitee_id_fkey
ALTER TABLE roles VALIDATE CONSTRAINT "roles_invitee_id_fkey";

-- [90/136] Validating roles.roles_menu_id_fkey
ALTER TABLE roles VALIDATE CONSTRAINT "roles_menu_id_fkey";
-- Progress: 90/136 completed

-- [91/136] Validating roles.roles_shop_id_fkey
ALTER TABLE roles VALIDATE CONSTRAINT "roles_shop_id_fkey";

-- [92/136] Validating ext_menu_item_soldout.ext_menu_item_soldout_ext_menu_id_fkey
ALTER TABLE ext_menu_item_soldout VALIDATE CONSTRAINT "ext_menu_item_soldout_ext_menu_id_fkey";

-- [93/136] Validating ext_menu_item_image.ext_menu_item_image_menu_item_image_id_fkey
ALTER TABLE ext_menu_item_image VALIDATE CONSTRAINT "ext_menu_item_image_menu_item_image_id_fkey";

-- [94/136] Validating ext_menu_item_image.ext_menu_item_img_ext_menu_id_fkey
ALTER TABLE ext_menu_item_image VALIDATE CONSTRAINT "ext_menu_item_img_ext_menu_id_fkey";

-- [95/136] Validating cur_option_set_schema_lang.cur_option_set_schema_lang_cur_option_set_schema_id_fkey
ALTER TABLE cur_option_set_schema_lang VALIDATE CONSTRAINT "cur_option_set_schema_lang_cur_option_set_schema_id_fkey";

-- [96/136] Validating cur_option_set_schema_lang.cur_option_set_schema_lang_opts_set_schema_id_fkey
ALTER TABLE cur_option_set_schema_lang VALIDATE CONSTRAINT "cur_option_set_schema_lang_opts_set_schema_id_fkey";

-- [97/136] Validating cur_menu_lang.cur_menu_lang_cur_menu_id_fkey
ALTER TABLE cur_menu_lang VALIDATE CONSTRAINT "cur_menu_lang_cur_menu_id_fkey";

-- [98/136] Validating ext_menu_item_section_change.ext_menu_item_section_change_ext_menu_item_id_fkey
ALTER TABLE ext_menu_item_section_change VALIDATE CONSTRAINT "ext_menu_item_section_change_ext_menu_item_id_fkey";

-- [99/136] Validating ext_menu_item_section_change.ext_menu_item_section_change_ext_menu_section_id_fkey
ALTER TABLE ext_menu_item_section_change VALIDATE CONSTRAINT "ext_menu_item_section_change_ext_menu_section_id_fkey";

-- [100/136] Validating ext_menu_touch_key_class.ext_menu_touch_key_class_ext_menu_id_fkey
ALTER TABLE ext_menu_touch_key_class VALIDATE CONSTRAINT "ext_menu_touch_key_class_ext_menu_id_fkey";
-- Progress: 100/136 completed

-- [101/136] Validating diffs_ext_cur.fk_cur_menu
ALTER TABLE diffs_ext_cur VALIDATE CONSTRAINT "fk_cur_menu";

-- [102/136] Validating diffs_ext_cur.fk_ext_menu
ALTER TABLE diffs_ext_cur VALIDATE CONSTRAINT "fk_ext_menu";

-- [103/136] Validating diffs_ext_cur.fk_owner
ALTER TABLE diffs_ext_cur VALIDATE CONSTRAINT "fk_owner";

-- [104/136] Validating incoming_requests.public_incoming_requests_menu_id_fkey
ALTER TABLE incoming_requests VALIDATE CONSTRAINT "public_incoming_requests_menu_id_fkey";

-- [105/136] Validating incoming_requests.public_incoming_requests_owner_id_fkey
ALTER TABLE incoming_requests VALIDATE CONSTRAINT "public_incoming_requests_owner_id_fkey";

-- [106/136] Validating incoming_requests.public_incoming_requests_shop_id_fkey
ALTER TABLE incoming_requests VALIDATE CONSTRAINT "public_incoming_requests_shop_id_fkey";

-- [107/136] Validating shops.shops_org_owner_id_fkey
ALTER TABLE shops VALIDATE CONSTRAINT "shops_org_owner_id_fkey";

-- [108/136] Validating shops.shops_owner_id_fkey
ALTER TABLE shops VALIDATE CONSTRAINT "shops_owner_id_fkey";

-- [109/136] Validating ext_option_schema.ext_option_schema_cur_id_fkey
ALTER TABLE ext_option_schema VALIDATE CONSTRAINT "ext_option_schema_cur_id_fkey";

-- [110/136] Validating ext_option_schema.ext_option_schema_opts_schema_id_fkey
ALTER TABLE ext_option_schema VALIDATE CONSTRAINT "ext_option_schema_opts_schema_id_fkey";
-- Progress: 110/136 completed

-- [111/136] Validating owners.owners_bot_id_fkey
ALTER TABLE owners VALIDATE CONSTRAINT "owners_bot_id_fkey";

-- [112/136] Validating menu_item_opts_set.menu_item_opts_set_menu_item_id_fkey
ALTER TABLE menu_item_opts_set VALIDATE CONSTRAINT "menu_item_opts_set_menu_item_id_fkey";

-- [113/136] Validating menu_item_opts_set.menu_item_opts_set_schema_id_fkey
ALTER TABLE menu_item_opts_set VALIDATE CONSTRAINT "menu_item_opts_set_schema_id_fkey";

-- [114/136] Validating menu_item_opts_set_schema.menu_item_opts_set_schema_menu_id_fkey
ALTER TABLE menu_item_opts_set_schema VALIDATE CONSTRAINT "menu_item_opts_set_schema_menu_id_fkey";

-- [115/136] Validating cur_option_set.cur_option_set_cur_menu_item_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_cur_menu_item_id_fkey";

-- [116/136] Validating cur_option_set.cur_option_set_cur_option_set_schema_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_cur_option_set_schema_id_fkey";

-- [117/136] Validating cur_option_set.cur_option_set_ext_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_ext_id_fkey";

-- [118/136] Validating cur_option_set.cur_option_set_menu_item_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_menu_item_id_fkey";

-- [119/136] Validating cur_option_set.cur_option_set_opts_set_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_opts_set_id_fkey";

-- [120/136] Validating cur_option_set.cur_option_set_opts_set_schema_id_fkey
ALTER TABLE cur_option_set VALIDATE CONSTRAINT "cur_option_set_opts_set_schema_id_fkey";
-- Progress: 120/136 completed

-- [121/136] Validating owner_agents.owner_agents_owner_id_fkey
ALTER TABLE owner_agents VALIDATE CONSTRAINT "owner_agents_owner_id_fkey";

-- [122/136] Validating cur_option_schema_lang.cur_option_schema_lang_cur_option_schema_id_fkey
ALTER TABLE cur_option_schema_lang VALIDATE CONSTRAINT "cur_option_schema_lang_cur_option_schema_id_fkey";

-- [123/136] Validating cur_option_schema_lang.cur_option_schema_lang_ext_id_fkey
ALTER TABLE cur_option_schema_lang VALIDATE CONSTRAINT "cur_option_schema_lang_ext_id_fkey";

-- [124/136] Validating menu_item_opts.menu_item_opts_schema_id_fkey
ALTER TABLE menu_item_opts VALIDATE CONSTRAINT "menu_item_opts_schema_id_fkey";

-- [125/136] Validating menu_item_opts.menu_item_opts_set_id_fkey
ALTER TABLE menu_item_opts VALIDATE CONSTRAINT "menu_item_opts_set_id_fkey";

-- [126/136] Validating ext_shop.ext_shop_ext_system_id_fkey
ALTER TABLE ext_shop VALIDATE CONSTRAINT "ext_shop_ext_system_id_fkey";

-- [127/136] Validating ext_shop.ext_shop_shop_id_fkey
ALTER TABLE ext_shop VALIDATE CONSTRAINT "ext_shop_shop_id_fkey";

-- [128/136] Validating cur_menu_item_image.cur_menu_item_image_cur_menu_id_fkey
ALTER TABLE cur_menu_item_image VALIDATE CONSTRAINT "cur_menu_item_image_cur_menu_id_fkey";

-- [129/136] Validating cur_menu_item_image.cur_menu_item_image_cur_menu_item_id_fkey
ALTER TABLE cur_menu_item_image VALIDATE CONSTRAINT "cur_menu_item_image_cur_menu_item_id_fkey";

-- [130/136] Validating cur_menu_item_image.cur_menu_item_image_ext_id_fkey
ALTER TABLE cur_menu_item_image VALIDATE CONSTRAINT "cur_menu_item_image_ext_id_fkey";
-- Progress: 130/136 completed

-- [131/136] Validating cur_menu_item_image.cur_menu_item_image_menu_item_id_fkey
ALTER TABLE cur_menu_item_image VALIDATE CONSTRAINT "cur_menu_item_image_menu_item_id_fkey";

-- [132/136] Validating cur_menu_item_image.cur_menu_item_image_menu_item_image_id_fkey
ALTER TABLE cur_menu_item_image VALIDATE CONSTRAINT "cur_menu_item_image_menu_item_image_id_fkey";

-- [133/136] Validating cur_menu_section.cur_menu_section_cur_menu_id_fkey
ALTER TABLE cur_menu_section VALIDATE CONSTRAINT "cur_menu_section_cur_menu_id_fkey";

-- [134/136] Validating cur_menu_section.cur_menu_section_ext_id_fkey
ALTER TABLE cur_menu_section VALIDATE CONSTRAINT "cur_menu_section_ext_id_fkey";

-- [135/136] Validating cur_menu_section.cur_menu_section_menu_section_id_fkey
ALTER TABLE cur_menu_section VALIDATE CONSTRAINT "cur_menu_section_menu_section_id_fkey";

-- [136/136] Validating ext_option_schema_lang.ext_option_schema_lang_cur_id_fkey
ALTER TABLE ext_option_schema_lang VALIDATE CONSTRAINT "ext_option_schema_lang_cur_id_fkey";

-- All FK constraints validated!
