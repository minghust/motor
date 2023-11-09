// Author: Ming Zhang
// Copyright (c) 2023

#include "process/txn.h"

size_t TXN::CollectDeleteNewestAttr(AttrPos* attr_pos, bitmap_t read_pos_bmp, table_id_t table_id) {
  size_t must_read_attrs_len = 0;
  offset_t offset_in_struct = 0;

  uint64_t mask = 0x1;
  for (int attr_idx = 1; attr_idx <= ATTRIBUTE_NUM[table_id]; attr_idx++, mask <<= 1) {
    offset_in_struct += ATTR_SIZE[table_id][attr_idx - 1];
    if (read_pos_bmp & mask) {
      // Read vcell[next_pos]'s modified attributes. These attributes must be read
      must_read_attrs_len += ATTR_SIZE[table_id][attr_idx];
      attr_pos->offs_within_struct.push_back(offset_in_struct);
      attr_pos->lens.push_back(ATTR_SIZE[table_id][attr_idx]);
    }
  }

  return must_read_attrs_len;
}

size_t TXN::CollectDeleteMiddleAttr(AttrPos* attr_pos, CVT* cvt, int read_pos, table_id_t table_id) {
  size_t must_read_attrs_len = 0;
  offset_t offset_in_struct = 0;
  bitmap_t read_pos_bmp = cvt->vcell[read_pos].attri_bitmap;

  uint64_t mask = 0x1;
  for (int attr_idx = 1; attr_idx <= ATTRIBUTE_NUM[table_id]; attr_idx++, mask <<= 1) {
    offset_in_struct += ATTR_SIZE[table_id][attr_idx - 1];
    if (read_pos_bmp & mask) {
      // This attribute should not be modified by newer version
      if (!IsFurtherModified(mask, cvt, read_pos)) {
        must_read_attrs_len += ATTR_SIZE[table_id][attr_idx];
        attr_pos->offs_within_struct.push_back(offset_in_struct);
        attr_pos->lens.push_back(ATTR_SIZE[table_id][attr_idx]);
      }
    }
  }

  return must_read_attrs_len;
}

bool TXN::IsFurtherModified(uint64_t mask, CVT* cvt, int read_pos) {
  for (int vc_id = (read_pos + 1) % MAX_VCELL_NUM;
       (cvt->vcell[vc_id].valid) && (cvt->vcell[vc_id].version > cvt->vcell[read_pos].version);
       vc_id = (vc_id + 1) % MAX_VCELL_NUM) {
    auto bitmap = cvt->vcell[vc_id].attri_bitmap;
    if (bitmap & mask) {
      return true;
    }
  }

  return false;
}

void TXN::CollectAttr(std::vector<AttrRead>& attr_read_list,
                      AttrPos* attr_pos,
                      // use pointer to reduce copy into pending_value_read
                      std::vector<OldAttrPos>* old_attr_pos,
                      table_id_t table_id,
                      CVT* cvt,
                      int next_pos,
                      DataSetItem* item_ptr) {
  bitmap_t next_pos_bmp = cvt->vcell[next_pos].attri_bitmap;

  size_t must_read_attrs_len = 0;
  offset_t offset_in_struct = 0;

  uint64_t mask = 0x1;
  for (int attr_idx = 1; attr_idx <= ATTRIBUTE_NUM[table_id]; attr_idx++, mask <<= 1) {
    offset_in_struct += ATTR_SIZE[table_id][attr_idx - 1];
    if (next_pos_bmp & mask) {
      // Read vcell[next_pos]'s modified attributes. These attributes must be read
      must_read_attrs_len += ATTR_SIZE[table_id][attr_idx];
      attr_pos->offs_within_struct.push_back(offset_in_struct);
      attr_pos->lens.push_back(ATTR_SIZE[table_id][attr_idx]);
    } else {
      // The modified attr_idx-th attribute is not in vcell[next_pos]
      SearchOldVCells(attr_idx, mask, attr_read_list, old_attr_pos, table_id, cvt, next_pos);
    }
  }

  assert(must_read_attrs_len != 0);

  char* must_read_attrs_buf = thread_rdma_buffer_alloc->Alloc(must_read_attrs_len);
  attr_pos->local_attr_buf = must_read_attrs_buf;

  attr_read_list.emplace_back(
      AttrRead{.local_attr_buf = must_read_attrs_buf,
               .remote_attr_off = cvt->vcell[next_pos].attri_so + cvt->header.remote_attribute_offset,
               .attr_size = must_read_attrs_len});
}

void TXN::SearchOldVCells(int attr_idx,
                          uint64_t mask,
                          std::vector<AttrRead>& attr_read_list,
                          std::vector<OldAttrPos>* old_attr_pos,
                          table_id_t table_id,
                          CVT* cvt,
                          int next_pos) {
  // We need to check which old vcell contains this modification, from old to new.
  // The vcell id should >=0. The vcell should be valid. The vcell's version should > start_time to collect future-than-me undos
  for (int vc_id = (next_pos + 1) % MAX_VCELL_NUM;
       (cvt->vcell[vc_id].valid) && (cvt->vcell[vc_id].version > start_time);
       vc_id = (vc_id + 1) % MAX_VCELL_NUM) {
    auto bitmap = cvt->vcell[vc_id].attri_bitmap;
    // Check whether this vcell contains the attr_idx-th attribute
    if (bitmap & mask) {
      // If this vcell contains the modified attribute, I need to calculate the remote address of this attribute.
      // To achieve this, I need to first calculate the offset of this attribute in all the modified attributes of this vcell
      // This offset is called attr_inner_off
      // Moreover, I also need to calculate the offset this attribute in the DB record for future apply&copy value.
      // This offset is called off_within_struct

      offset_t attr_inner_off = 0;
      offset_t off_within_struct = 0;

      // If attr_idx is 1, attr_inner_off and off_within_struct should be 0
      // If attr_idx is not 1:
      //   (1) attr_inner_off is the total size of modified (i.e., `1`) attributes ranked before the attr_idx-th attribute
      //   (2) off_within_struct is the total size of all attributes ranked before the attr_idx-th attribute

      uint64_t inner_mask = 0x1;
      for (int i = 1; i < attr_idx; i++) {
        off_within_struct += ATTR_SIZE[table_id][i];

        if (bitmap & inner_mask) {
          attr_inner_off += ATTR_SIZE[table_id][i];
        }

        inner_mask <<= 1;
      }

      size_t attr_sz = ATTR_SIZE[table_id][attr_idx];
      char* attr_buf = thread_rdma_buffer_alloc->Alloc(attr_sz);

      // Used to send RDMA read to fetch one attribute
      attr_read_list.emplace_back(
          AttrRead{.local_attr_buf = attr_buf,
                   .remote_attr_off = cvt->vcell[vc_id].attri_so + attr_inner_off + cvt->header.remote_attribute_offset,
                   .attr_size = attr_sz});

      // Used to locate which position should
      // this attribute be copied into the fetched full value
      old_attr_pos->emplace_back(
          OldAttrPos{.local_attr_buf = attr_buf,
                     .off_within_struct = off_within_struct,
                     .len = attr_sz});

      return;
    }
  }
}
