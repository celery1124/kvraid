/* ec.h
* 06/23/2019
* by Mian Qin
*/

#ifndef   _ec_h_   
#define   _ec_h_   

#include <stdlib.h>
#include "jerasure/galois.h"
#include "jerasure/jerasure.h"
#include "jerasure/reed_sol.h"

#define EC_codeword 8
class EC {
private:
    int k_;
    int r_;
    int* matrix_;
public:
    EC() {}
    EC(int num_d, int num_p) :
    k_(num_d), r_(num_p) { };
    void setup () {
        matrix_ = reed_sol_vandermonde_coding_matrix(k_, r_, EC_codeword);
    }
    int get_num_data() {return k_;}
    int get_num_parity() {return r_;}
    int encode(char** data, char** coding, int chunk_size) {
        jerasure_matrix_encode(k_, r_, EC_codeword, matrix_, data, coding, chunk_size);
        return 0;
    }
    // erased start from 0
    int single_failure_decode(int erased, char** data, char** coding, int chunk_size) {
        int erasures[2] = {-1,-1};
        erasures[0] = erased;
        return jerasure_matrix_decode(k_, r_, EC_codeword, matrix_, 1, erasures, data, coding, chunk_size);
    }

    // patch update to code
    int update(int col_index, char* data_old, char* data_new, char** coding, int chunk_size) {
        if (data_old != NULL){ // substruct old data;
            galois_region_xor(data_old, data_new, chunk_size);
        }
        for (int i = 0; i<r_; i++) {
            int coeff = matrix_[col_index+i*k_];
            galois_w08_region_multiply(data_new, coeff, chunk_size, coding[i], 1);
        }

        return 0;
    }
};

#endif 