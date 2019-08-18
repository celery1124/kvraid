/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_yahoo_ycsb_db_KVredund */

#ifndef _Included_com_yahoo_ycsb_db_KVredund
#define _Included_com_yahoo_ycsb_db_KVredund
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    init
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_init
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    close
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_close
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    getBatchSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL Java_com_yahoo_ycsb_db_KVredund_getBatchSize
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    get
 * Signature: ([BI)[B
 */
JNIEXPORT jbyteArray JNICALL Java_com_yahoo_ycsb_db_KVredund_get
  (JNIEnv *, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    insert
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_insert
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    update
 * Signature: ([BI[BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_update
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    delete
 * Signature: ([BI)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_delete
  (JNIEnv *, jobject, jbyteArray, jint);

/*
 * Class:     com_yahoo_ycsb_db_KVredund
 * Method:    writeBatch
 * Signature: (Lcom/yahoo/ycsb/db/WriteBatch;)Z
 */
JNIEXPORT jboolean JNICALL Java_com_yahoo_ycsb_db_KVredund_writeBatch
  (JNIEnv *, jobject, jobject);

#ifdef __cplusplus
}
#endif
#endif