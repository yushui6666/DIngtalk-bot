"""角色识别单测：各角色解析、外部账号、角色重叠校验。"""

import pytest

from config import LISTENER_USER_ID, USER_ID_MAP
from models import ROLE_ENGINEER, ROLE_MANAGER, ROLE_OTHER, ROLE_SYSTEM, ROLE_UNKNOWN
from role_resolver import resolve_role, validate_role_overlap

NEI_YU_QING_OID = "DV2iipykTJciSappVW4GfsiSQii2iPTIiiIm8jw"
NEI_YU_QING_UID = "1785387642795212"
YUSHUI_OID = "Dk7Rf4NfFahnD2MHQgAE3gy2iPTIiiIm8jw"  # 外部测试账号，无 userId

GROUP_WITH_ALL = {
    "group_id": "g1",
    "manager_ids": ["uid-manager-1"],
    "engineer_ids": ["uid-engineer-1"],
    "other_member_ids": ["uid-other-1"],
}


def test_system_account():
    assert resolve_role(GROUP_WITH_ALL, LISTENER_USER_ID, USER_ID_MAP) == ROLE_SYSTEM


def test_manager_via_mapping():
    # openDingtalkId → userId 映射后命中 manager
    id_map = {"oid-manager": "uid-manager-1"}
    assert resolve_role(GROUP_WITH_ALL, "oid-manager", id_map) == ROLE_MANAGER


def test_engineer_via_mapping():
    id_map = {"oid-engineer": "uid-engineer-1"}
    assert resolve_role(GROUP_WITH_ALL, "oid-engineer", id_map) == ROLE_ENGINEER


def test_other_via_mapping():
    id_map = {"oid-other": "uid-other-1"}
    assert resolve_role(GROUP_WITH_ALL, "oid-other", id_map) == ROLE_OTHER


def test_no_mapping_unknown():
    # 无 userId 映射（外部账号如 yushui）→ UNKNOWN
    assert resolve_role(GROUP_WITH_ALL, YUSHUI_OID, USER_ID_MAP) == ROLE_UNKNOWN


def test_no_group_config():
    assert resolve_role(None, "anything", USER_ID_MAP) == ROLE_UNKNOWN


def test_mapping_to_nonexistent_user():
    # 映射存在但 userId 不在任何角色列表
    id_map = {"oid-x": "uid-not-listed"}
    assert resolve_role(GROUP_WITH_ALL, "oid-x", id_map) == ROLE_UNKNOWN


def test_role_overlap_raises():
    bad = {"group_id": "g2", "manager_ids": ["same-uid"], "engineer_ids": ["same-uid"]}
    with pytest.raises(ValueError):
        validate_role_overlap(bad)


def test_role_overlap_ok():
    validate_role_overlap(GROUP_WITH_ALL)  # 不抛异常
