//
// Copywrite 2016 Institute of Computing Technology, CAS
// By Fei Xia,ICT,CAS. xiafei2011@ict.ac.cn
//

#ifndef NVMKV_BPTREE_H_
#define NVMKV_BPTREE_H_

#include <stdio.h>
#include <stdlib.h>
#include <queue>
#include <immintrin.h>

#include "tbb/spin_rw_mutex.h"

#include "util.h"

// #define USE_RWLOCK
#define BPTREE_ELEMENT_LENGTH 8
#define BPTREE_VALUE_LENGTH 8

#define BPTREE_KEY_MASK (((size_t)1 << 8) - 1)
#define BPTREE_ITEM_KEY_LENGTH(element_item) (element_item >> 24) & BPTREE_KEY_MASK

typedef tbb::speculative_spin_rw_mutex spec_rw_mutex_t;
typedef tbb::spin_rw_mutex rw_mutex_t;

namespace nvmkv
{
class Bptree 
{
public:
	static const uint32_t max_inner_slots = 128; 
	static const uint32_t max_leaf_slots = 64;	

	static const size_t max_key_length = MAX_KEY_LENGTH; 
	static const size_t leaf_item_size = BPTREE_ELEMENT_LENGTH + max_key_length + BPTREE_VALUE_LENGTH;

	struct Key 
	{
		uint8_t key_length;
		uint8_t data[max_key_length];
		struct Key operator =(struct Key &key)
		{
			this->key_length = key.key_length;
			memcpy8(this->data, key.data, key.key_length);
			
			return *this;
		}
	};

	static int key_compare(const void *a, const void *b)
	{
		Key *keya = (Key *)a;
		Key *keyb = (Key *)b; 
		return key_memcmp8((const uint8_t *)keya->data, (const uint8_t *)keyb->data, (size_t)keya->key_length, (size_t)keyb->key_length);
	}

	struct KV_item
	{
		uint32_t key_length; 
		uint8_t key[max_key_length];
		uint64_t value;

		struct KV_item operator = (struct KV_item &item)
		{
			this->key_length = item.key_length;
			memcpy8(this->key, item.key, key_length);
			this->value = item.value;

			return *this;
		}
	};

	struct node
	{
		uint16_t level;
		uint32_t nkeys;

		inline void initialize(uint16_t l)
		{
			level = l;
			nkeys = 0;
		}

		inline bool isleafnode() 
		{
			return (level == 0);
		}
	};

	//struct alignas(64) inner_node : public node 
	struct inner_node : public node 
	{
		Key keys[max_inner_slots];
		struct node *childid[max_inner_slots + 1];	

		inline void initialize(uint16_t l) 
		{
			node::initialize(l);
		}

		inline bool isfull()
		{
			return nkeys == max_inner_slots;
		}

		inline bool isunderflow() const 
		{
			return (nkeys < max_inner_slots / 2);
		}

		inline bool isfew() const 
		{
			return (nkeys <= max_inner_slots / 2);
		}
	};

	//struct alignas(64) leaf_node : public node 
	struct leaf_node : public node 
	{
		struct leaf_node *next;
		rw_mutex_t rwlock;
		volatile uint32_t _lock;
		KV_item kv_items[max_leaf_slots];

		inline void lock()
		{
		#ifdef USE_RWLOCK
			this->rwlock.lock();
		#else
			while(1)
			{
				if(__sync_bool_compare_and_swap((volatile uint32_t*)&_lock, 0U, 1U))
					break;
			}
		#endif
		}

		inline bool try_lock()
		{
		#ifdef USE_RWLOCK
			if(!rwlock.try_lock()) {
				return false;
			} else {
				return true;
			}
		#else
			if (__sync_bool_compare_and_swap((volatile uint32_t*)&_lock, 0U, 1U))
			{	
				return true;
			}
			else
				return false;
		#endif
		}

		inline void unlock()
		{
		#ifdef USE_RWLOCK	
			this->rwlock.unlock();
		#else 
			memory_barrier();
			assert(((volatile uint32_t)_lock & 1U) == 1U);
			_lock = 0U;
		#endif
		}
		
		inline void initialize() 
		{
			node::initialize(0);
			next = NULL;
			//lock = 0; 
		}

		inline bool isfull()
		{
			return nkeys == max_leaf_slots;
		}

		inline bool isunderflow() const 
		{
			return (nkeys < max_leaf_slots / 2);
		}

		inline bool isfew() const 
		{
			return (nkeys <= max_leaf_slots / 2);
		}

	};

	struct bptree_iter_t
	{
		struct leaf_node *leaf;
		uint32_t slot;
		uint32_t key_length;
		uint8_t key[max_key_length];
		uint64_t value;
	};	

	//typedef struct leaf_node_ leaf_node;
	enum result_flags_t 
	{
		// deletion is successful, and not need to update parent.
		bptree_ok = 0,
		// deletion is not successful, because key was not found.
		key_not_found = 1,
		// deletion is successful, the last key of leaf was updated, 
		// so needs to update parent. 
		update_lastkey = 2,
		// deletion is successful, children nodes were merged, 
		// so the parent needs to remove the empty index.
		bptree_fixmerge = 4,

		leaf_full = 8,
		
		leaf_empty = 0x10
	};

	struct result_t
	{
		result_flags_t flags;
		// assuming the max_key_length is 32.
		uint8_t lastkey[32];
		size_t lastkey_length;
		inline result_t(result_flags_t f = bptree_ok)
			: flags(f)
		{}

		inline result_t(result_flags_t f, const uint8_t *key, size_t length)
			: flags(f)
		{
			assert(length <= 32);
			memcpy8(lastkey, key, length);
			lastkey_length = length;	
		}
		
		inline bool has(result_flags_t f) const 
		{
			return ((flags & f) != 0);
		}

		inline result_t& operator|= (const result_t &other)
		{
			flags = result_flags_t(flags | other.flags);

			if (other.has(update_lastkey))
			{
				memcpy8(lastkey, other.lastkey, other.lastkey_length);
				lastkey_length = other.lastkey_length;
			}
		
			return *this;
		}
	};

	struct bptree_ulog
	{
		leaf_node *current_leaf;
		leaf_node *new_leaf;	
		leaf_node *prev_leaf;
	};

#ifdef BPTREE_COLLECT_STATS
	struct bptree_stats
	{
		uint32_t tree_height;
		uint32_t total_leaf_nodes;
		uint32_t total_inner_nodes;

		uint64_t lock_time;
		uint64_t index_time; 
		uint64_t access_time;
		uint64_t persist_time;
	};
#endif

private:
	node *bptree_root;
	// head of the linked list of leaves
	leaf_node *leaf_list_header;

#ifdef BPTREE_COLLECT_STATS
	bptree_stats stats;
#endif

private:
	
	leaf_node* bptree_allocate_leaf()
	{
		leaf_node *leaf = (leaf_node *)malloc(sizeof(leaf_node));
		leaf->initialize();

#ifdef BPTREE_COLLECT_STATS
		stats.total_leaf_nodes++;
#endif
		
		return leaf;
	}

	inner_node* bptree_allocate_in(uint32_t level)
	{
		inner_node *inner = (inner_node *)malloc(sizeof(inner_node));
		inner->initialize(level);	

#ifdef BPTREE_COLLECT_STATS
		stats.total_inner_nodes++;
#endif
		
		return inner;
	}

	leaf_node* bptree_new_tree()
	{
		leaf_node *leaf = bptree_allocate_leaf();	
		bptree_root = (struct node*)leaf;
		leaf_list_header = leaf;

		return leaf;
	}

	size_t inner_binary_search(inner_node *inner, uint8_t *key, size_t key_length)
	{
		int slot = 0;
		int high = (int)inner->nkeys - 1;
		int low = 0;
		while (low <= high)
		{
			int medium = (high - low) / 2 + low;	
			int ret = key_memcmp8(key, inner->keys[medium].data, key_length, (size_t)(inner->keys[medium].key_length));
			if (ret == 0)
			{
				slot = medium;
				break;
			}
			else if (ret < 0)
			{
				high = medium - 1;
				slot = medium;
			}
			else
			{
				low = medium + 1;
				slot = low;
			}
		}

		return (size_t)slot;
	}

	void find_leaf_and_prevleaf(uint8_t *key, size_t key_length, leaf_node **leaf, leaf_node **prev_leaf)
	{
		struct node *n = bptree_root;
		struct node *prev = NULL;
		while (!n->isleafnode())	
		{
			inner_node *inner = static_cast<inner_node *>(n);
			size_t slot = inner_binary_search(inner, key, key_length);
			n = inner->childid[slot];

			if (slot > 0)
			{
				prev = inner->childid[slot - 1];
			}
			else if (prev)
			{
				inner = static_cast<inner_node *>(prev);
				prev = inner->childid[inner->nkeys];
			}
		}

		*leaf = static_cast<leaf_node *>(n);
		if (prev)
		{
			*prev_leaf = static_cast<leaf_node *>(prev);
		}
	}

	void insert_kv_item(leaf_node *leaf, uint32_t slot, uint8_t *key, size_t key_length, uint64_t value)
	{
			KV_item *kv_item = &(leaf->kv_items[slot]);

			kv_item->key_length = key_length;
			memcpy8(kv_item->key, key, key_length);
			kv_item->value = value;
	}

	void copy_leaf_node(leaf_node *dest_leaf, leaf_node *src_leaf, uint32_t start, uint32_t end)
	{
		uint32_t src_slot = 0;
		uint32_t dest_slot = 0;
		for (src_slot = start; src_slot < end; src_slot++)
		{
			dest_leaf->kv_items[dest_slot] = src_leaf->kv_items[src_slot];
			dest_slot++;
		}	
	}

	void split_leaf_node(leaf_node *leaf, Key *splitkey, leaf_node **_newleaf)
	{
		uint32_t mid = (leaf->nkeys >> 1);

		leaf_node *newleaf = bptree_allocate_leaf();

		newleaf->nkeys = leaf->nkeys - mid;
		newleaf->next = leaf->next;
				
		//copy_leaf_node(newleaf, leaf, mid, leaf->nkeys);	
		std::copy(leaf->kv_items + mid, leaf->kv_items + leaf->nkeys, newleaf->kv_items);
		
		leaf->nkeys = mid;
		leaf->next = newleaf;
		
		// set the splitkey and _newleaf 
		KV_item *item = &(leaf->kv_items[leaf->nkeys - 1]);
		(*splitkey).key_length = item->key_length;
		memcpy8((*splitkey).data, item->key, item->key_length);

		*_newleaf = newleaf;
	}

	result_t delete_parent_recursive(uint8_t *key, size_t key_length, struct node *cur, struct node *left, struct node *right, inner_node *leftparent, inner_node *rightparent, inner_node *parent, size_t parentslot)
	{
		if (cur->isleafnode())
		{
			leaf_node *leaf = static_cast<leaf_node *>(cur);
			leaf_node *leftleaf = static_cast<leaf_node *>(left);
			leaf_node *rightleaf = static_cast<leaf_node *>(right);

			if (leaf == bptree_root)
			{
				// if the empty leaf is the root, then delete all nodes.
				if (leftleaf == NULL && rightleaf == NULL)
				{
					bptree_root = NULL;
					leaf_list_header = NULL;

					return bptree_ok;
				}
			}
			
			return bptree_fixmerge;
		}
		else // !cur->isleafnode()
		{
			inner_node *inner = static_cast<inner_node *>(cur);
			inner_node *leftin = static_cast<inner_node *>(left);
			inner_node *rightin = static_cast<inner_node *>(right);
				
			struct node *myleft, *myright;
			inner_node *myleftparent, *myrightparent;

			size_t slot = inner_binary_search(inner, key, key_length);
			if (slot == 0)
			{
				myleft = (left == NULL) ? NULL : leftin->childid[leftin->nkeys];
				myleftparent = leftparent; 
			}
			else
			{
				myleft = inner->childid[slot - 1];
				myleftparent = inner;	
			}
			if (slot == inner->nkeys)
			{
				myright = (right == NULL) ? NULL : rightin->childid[0];
				myrightparent = rightparent; 
			}
			else
			{
				myright = inner->childid[slot + 1];
				myrightparent = inner;	
			}
			
			result_t result = delete_parent_recursive(key, key_length, inner->childid[slot], myleft, myright, myleftparent, myrightparent, inner, slot);	

			result_t myres = bptree_ok;

			if (result.has(bptree_fixmerge))
			{
				// the current node next is empty, then remove it.
				struct node *child = inner->childid[slot];
				if (child->isleafnode()) // the leaf will be deleted later.
				{
					assert(child->nkeys == 1);
					std::copy(inner->keys + slot+1, inner->keys + inner->nkeys, inner->keys + slot);	
					std::copy(inner->childid + slot+1, inner->childid + inner->nkeys+1, inner->childid + slot);
				//	slot++;
				}
				else // inner node
				{
					inner_node *childinner = static_cast<inner_node *>(child);
					if (childinner->nkeys != 0)
						slot++;

					//assert(inner->childid[slot]->nkeys == 0);
					free(inner->childid[slot]);	

				std::copy(inner->keys + slot, inner->keys + inner->nkeys, inner->keys + slot - 1);	
				std::copy(inner->childid + slot+1, inner->childid + inner->nkeys+1, inner->childid + slot);
				}
			
				inner->nkeys--;
			}

			if (inner->isunderflow() && !(inner == bptree_root && inner->nkeys >= 1))
			{
				// the inner node is the root and has just one child.
				// the child becomes the new root 
				if (leftin == NULL && rightin == NULL)
				{
					assert(inner == bptree_root);
					assert(inner->nkeys == 0);
					bptree_root = inner->childid[0];
					
					inner->nkeys = 0;
					free(inner);
				
					return bptree_ok;
				}
				// both left and right leaves are underflow, then merge the more local inner node
				else if ((leftin == NULL || leftin->isfew()) && (rightin == NULL || rightin->isfew()))
				{
					if (leftparent == parent)
						myres |= merge_inner_nodes(leftin, inner, leftparent, parentslot - 1);
					else
						myres |= merge_inner_nodes(inner, rightin, rightparent, parentslot);
				}
				// the right leaf has extra data, so balance right with current node
				else if ((leftin != NULL && leftin->isfew()) && (rightin != NULL && !rightin->isfew()))
				{
					if (rightparent == parent)
						shift_left_inner(inner, rightin, rightparent, parentslot);
					else
						myres |= merge_inner_nodes(leftin, inner, leftparent, parentslot - 1);	
				}
				// the left leaf has extra data, so balance left with current node
				else if ((leftin != NULL && !leftin->isfew()) && (rightin != NULL && rightin->isfew()))
				{
					if (leftparent == parent)
						shift_right_inner(leftin, inner, leftparent, parentslot - 1);
					else
						myres |= merge_inner_nodes(inner, rightin, rightparent, parentslot);	
				}
				// both the left and right leaves have extra data
				else if (leftparent == rightparent)
				{
					if (leftin->nkeys <= rightin->nkeys)
						shift_left_inner(inner, rightin, rightparent, parentslot);
					else
						shift_right_inner(leftin, inner, leftparent, parentslot - 1);
				}
				else
				{
					if (leftparent == parent)
						shift_right_inner(leftin, inner, leftparent, parentslot - 1);
					else
						shift_left_inner(inner, rightin, rightparent, parentslot);
				}
			}
			
			return myres;
		}
	}

	void split_inner_node(inner_node *inner, Key *newkey, struct node **_newin, size_t slot)
	{
		size_t mid = (inner->nkeys >> 1);
		if (slot <= mid && mid > inner->nkeys - (mid + 1))
			mid--;

		inner_node *newin = bptree_allocate_in(inner->level);
		newin->nkeys = inner->nkeys - (mid + 1);

		// move items to new inner_node
		std::copy(inner->keys + mid+1, inner->keys + inner->nkeys, newin->keys);
		std::copy(inner->childid + mid+1, inner->childid + inner->nkeys+1, newin->childid);

		inner->nkeys = mid;

		*_newin = newin;
		*newkey = inner->keys[mid];
	}

	struct result_t merge_inner_nodes(inner_node *left, inner_node *right, inner_node *parent, size_t parentslot)
	{
		assert(left->level == right->level);
		assert(parent->level == left->level + 1);
		assert(left->nkeys + right->nkeys < max_inner_slots);

		left->keys[left->nkeys] = parent->keys[parentslot];
		left->nkeys++;

		std::copy(right->keys, right->keys + right->nkeys, left->keys + left->nkeys);	
		std::copy(right->childid, right->childid + right->nkeys + 1, left->childid + left->nkeys);

		left->nkeys += right->nkeys;
		right->nkeys = 0;

		return bptree_fixmerge;
	}

	void shift_left_inner(inner_node *left, inner_node *right, inner_node *parent, size_t parentslot)
	{
		assert(parent->level == left->level + 1);
		assert(left->nkeys < right->nkeys);

		size_t shiftnum = (right->nkeys - left->nkeys) >> 1;
		assert(left->nkeys + shiftnum < max_inner_slots);

		// copy the parent's decision key and childid to the first new key on the left
		left->keys[left->nkeys] = parent->keys[parentslot];
		left->nkeys++;

		// copy other items from right to the left
		std::copy(right->keys, right->keys + shiftnum-1, left->keys + left->nkeys);
		std::copy(right->childid, right->childid + shiftnum, left->childid + left->nkeys);

		left->nkeys += shiftnum - 1;
		
		// update parent decision key
		parent->keys[parentslot] = right->keys[shiftnum - 1];

		// shift all items inner the right
		std::copy(right->keys + shiftnum, right->keys + right->nkeys, right->keys);	
		std::copy(right->childid + shiftnum, right->childid + right->nkeys + 1, right->childid);

		right->nkeys -= shiftnum;
	}
		
	void shift_right_inner(inner_node *left, inner_node *right, inner_node *parent, size_t parentslot)
	{
		assert(parent->level == left->level + 1);
		assert(left->nkeys > right->nkeys);

		size_t shiftnum = (left->nkeys - right->nkeys) >> 1;
		assert(right->nkeys + shiftnum < max_inner_slots);

		// shift all items in the right
		std::copy_backward(right->keys, right->keys + right->nkeys,
											 right->keys + right->nkeys + shiftnum);
		std::copy_backward(right->childid, right->childid + right->nkeys+1,
											 right->childid + right->nkeys+1 + shiftnum);

		right->nkeys += shiftnum;
		
		// copy the parent's decision key and childid to the last new key on the right
		right->keys[shiftnum - 1] = parent->keys[parentslot];

		// copy items from left to the right
		std::copy(left->keys + left->nkeys - shiftnum+1, left->keys + left->nkeys,
										right->keys);
		std::copy(left->childid + left->nkeys - shiftnum+1, left->childid + left->nkeys+1,
										right->childid);

		// copy the first to-be-removed key from the left to the parent's decision postion.
		parent->keys[parentslot] = left->keys[left->nkeys - shiftnum];

		left->nkeys -= shiftnum;
	}

	bool update_parents_recursive(struct node *n, uint8_t *key, size_t key_length, Key *leaf_splitkey, leaf_node *new_leaf, Key *splitkey, struct node **splitnode)
	{
		if (!n->isleafnode())
		{
			inner_node *inner = static_cast<inner_node *>(n);
			Key newkey;
			struct node *newchild = NULL;

			size_t slot = inner_binary_search(inner, key, key_length);
			struct node *child = inner->childid[slot]; 

			update_parents_recursive(child, key, key_length, leaf_splitkey, new_leaf, &newkey, &newchild);

			if (newchild)
			{
				if (inner->isfull())
				{
					split_inner_node(inner, splitkey, splitnode, slot);
					inner_node *splitin = static_cast<inner_node *>(*splitnode);
					if (slot == inner->nkeys + 1 && inner->nkeys < splitin->nkeys)
					{
						inner->keys[inner->nkeys] = *splitkey;
						inner->childid[inner->nkeys + 1] = splitin->childid[0];
						inner->nkeys++;

						splitin->childid[0] = newchild;
						*splitkey = newkey;
			
						return true;
					}
					// insert the key to the new splitnode
					else if (slot >= inner->nkeys + 1)
					{
						slot -= inner->nkeys + 1;
						inner = static_cast<inner_node *>(*splitnode);
					}
				}

				// move items 
				assert(slot >= 0 && slot <= inner->nkeys);
				std::copy_backward(inner->keys + slot, inner->keys + inner->nkeys, inner->keys + inner->nkeys + 1);
				std::copy_backward(inner->childid + slot, inner->childid + inner->nkeys + 1, inner->childid + inner->nkeys + 2); 

				inner->keys[slot] = newkey;
				inner->childid[slot + 1] = newchild;
		
				inner->nkeys++;
			}
			return true;
		}
		else // n->isleafnode() == true
		{
			*splitkey = *leaf_splitkey;	
			*splitnode = new_leaf;
			return true;
		}
	}

	void bptree_free()
	{
		// free all inner_nodes and leaf_nodes
		if (bptree_root)
		{
			std::queue<struct node *> node_queue;	
			node_queue.push(bptree_root);
			while(!node_queue.empty())
			{
				struct node *n = node_queue.front();
				if (n->level >= 1) // inner_node
				{
					inner_node *inner = static_cast<inner_node *>(n);
					// if inner_node, then push childern to queue
					uint32_t slot = 0;
					for (slot = 0; slot < inner->nkeys + 1; slot++)
					{
						struct node *child = inner->childid[slot]; 
						node_queue.push(child);		
					}
					// free the inner_node
					free(inner);
				}
				else
				{
					// is a leaf_node, free it directly.
					leaf_node *leaf = static_cast<leaf_node *>(n);
					free(leaf);
				}
				node_queue.pop();
			} // end while ()
		} // end if (bptree_root)
	}
	
	leaf_node* find_leaf_node(uint8_t *key, size_t key_length)
	{
		struct node *n = bptree_root;
		while (!n->isleafnode())	
		{
			inner_node *inner = static_cast<inner_node *>(n);
			size_t slot = inner_binary_search(inner, key, key_length);
			n = inner->childid[slot];
		}

		return static_cast<leaf_node *>(n);
	}

	uint32_t leaf_binary_search(leaf_node *leaf, uint8_t *key, size_t key_length, bool *same_key)
	{
		int slot = 0;

		int high = (int)leaf->nkeys - 1;
		int low = 0;
		while (low <= high)
		{
			int medium = (high - low) / 2 + low;	
			
			KV_item *item = &(leaf->kv_items[medium]);
			int ret = key_memcmp8(key, item->key, key_length, item->key_length);
			if (ret == 0)
			{
				slot = medium;
				*same_key = true;
				break;
			}
			else if (ret < 0)
			{
				high = medium - 1;
				slot = medium;
			}
			else
			{
				low = medium + 1;
				slot = low;
			}
		}

		return (uint32_t)slot;
	}

public:
	// use the spec_mutex in the processor that support Intel TSX-NI
	spec_rw_mutex_t spec_mutex;
	
	Bptree()
	{
		bptree_root = NULL;
		leaf_list_header = NULL;
	}

	~Bptree()
	{
		bptree_free();
	}
	
	bool is_null() 
	{
		return this->bptree_root == NULL ? true : false;
	}

	static bool bptree_put(Bptree *bt, uint8_t *key, size_t key_length, uint64_t value)
	{
		leaf_node *leaf = NULL;
		
		while (1)
		{
			spec_rw_mutex_t::scoped_lock spec_lock;
			if (bt->bptree_root == NULL)
			{
				if (!spec_lock.try_acquire(bt->spec_mutex, true))
				{
					continue;
				}		
				leaf = bt->bptree_new_tree();
				spec_lock.release();
			}
			
			spec_lock.acquire(bt->spec_mutex, false);
			leaf = bt->find_leaf_node(key, key_length); 	
			spec_lock.release();
			
			if (!leaf->try_lock())
			{
				// spec_lock.release();
				// _xabort(0xff);
				continue;
			}
			
			result_flags_t result_flag = bptree_ok;
			if (leaf->isfull())
			{
				result_flag = leaf_full;
			}
			
			bool has_same_key = false;	
			uint32_t slot = bt->leaf_binary_search(leaf, key, key_length, &has_same_key);
			
			if (has_same_key)
			{
				// update value of the kv_item
				(leaf->kv_items[slot]).value = value;
				leaf->unlock();
				return true;
			}

			leaf_node *tgt_leaf = leaf;
			Key leaf_splitkey; 
			leaf_node *new_leaf = NULL;
			
			if (result_flag == leaf_full)
			{
				bt->split_leaf_node(leaf, &leaf_splitkey, &new_leaf);
				if (slot >= leaf->nkeys)
				{
					slot -= leaf->nkeys;
					tgt_leaf = new_leaf;
				}
			}	
	
			// put kv_item to the correct slot
			std::copy_backward(tgt_leaf->kv_items + slot, tgt_leaf->kv_items + tgt_leaf->nkeys, tgt_leaf->kv_items + tgt_leaf->nkeys+1);
			bt->insert_kv_item(tgt_leaf, slot, key, key_length, value);	
			tgt_leaf->nkeys++;

			// update parents
			if (result_flag == leaf_full)
			{
				spec_lock.acquire(bt->spec_mutex, true);
				
				Key splitkey;
				struct node *split_node = NULL;
				
				bt->update_parents_recursive(bt->bptree_root, key, key_length, &leaf_splitkey, new_leaf, &splitkey, &split_node);

				if (split_node)
				{
					inner_node *newroot = bt->bptree_allocate_in(bt->bptree_root->level + 1);	
					newroot->keys[0] = splitkey;
					newroot->childid[0] = bt->bptree_root;
					newroot->childid[1] = split_node;
					newroot->nkeys = 1;
					bt->bptree_root = newroot;
				}

				spec_lock.release();
			}

			leaf->unlock();
			break;
		} // end while(1)

		return true;
	}

	static bool bptree_delete(Bptree *bt, uint8_t *key, size_t key_length)
	{
		leaf_node *leaf = NULL;
		leaf_node *prev_leaf = NULL;
		result_flags_t result_flag = bptree_ok;

		while (1)
		{
			if (!bt->bptree_root)
				return false;
			
			spec_rw_mutex_t::scoped_lock spec_lock;
			spec_lock.acquire(bt->spec_mutex, false);
			bt->find_leaf_and_prevleaf(key, key_length, &leaf, &prev_leaf);		
			
			if (!leaf->try_lock())
			{
				spec_lock.release();
				continue;
			}
			
			if (leaf->nkeys == 1)
			{
				if (prev_leaf && (!prev_leaf->try_lock()))
				{
					leaf->unlock();
					spec_lock.release();
					continue;
				}
				result_flag = leaf_empty;	
			}
			
			spec_lock.release();
			break;

		} // end while (1)

		if (result_flag == leaf_empty)
		{
			spec_rw_mutex_t::scoped_lock spec_lock;
			spec_lock.acquire(bt->spec_mutex, true);
			bt->delete_parent_recursive(key, key_length, bt->bptree_root, NULL, NULL, NULL, NULL, NULL, 0);	
			spec_lock.release();
		
			if (bt->bptree_root)
			{
				if (leaf == bt->leaf_list_header) 
				{
					bt->leaf_list_header = leaf->next;
				}
				else
				{
					prev_leaf->next = leaf->next;
				}
				
				free(leaf);
			}
			if (prev_leaf)
			{
				prev_leaf->unlock();
			}
		}
		else
		{
			bool has_same_key = false;
			uint32_t slot = bt->leaf_binary_search(leaf, key, key_length, &has_same_key);
			if (has_same_key)
			{
				std::copy(leaf->kv_items + slot+1, leaf->kv_items + leaf->nkeys, leaf->kv_items + slot);	
				leaf->nkeys--;
			}
			leaf->unlock();
		}
		return true;
	}

	static uint64_t bptree_get(Bptree *bt, uint8_t *key, size_t key_length)
	{
		leaf_node *leaf = bt->find_leaf_node(key, key_length);
		bool same_key = false;
		int slot = bt->leaf_binary_search(leaf, key, key_length, &same_key);
		
		if (same_key == true) 
		{
			return leaf->kv_items[slot].value;
		}

		return -1LL;
	}

};

}

#endif
