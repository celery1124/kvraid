/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class com_yahoo_ycsb_db_WriteBatch */

#ifndef _Included_com_yahoo_ycsb_db_WriteBatch
#define _Included_com_yahoo_ycsb_db_WriteBatch
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_yahoo_ycsb_db_WriteBatch
 * Method:    init
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_init
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_WriteBatch
 * Method:    destory
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_destory
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_WriteBatch
 * Method:    put
 * Signature: ([BI[BIB)V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_put
  (JNIEnv *, jobject, jbyteArray, jint, jbyteArray, jint, jbyte);

/*
 * Class:     com_yahoo_ycsb_db_WriteBatch
 * Method:    clear
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_com_yahoo_ycsb_db_WriteBatch_clear
  (JNIEnv *, jobject);

/*
 * Class:     com_yahoo_ycsb_db_WriteBatch
 * Method:    getCppPtr
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_com_yahoo_ycsb_db_WriteBatch_getCppPtr
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
