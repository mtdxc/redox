#if defined(REDOX_STATICLIB) || defined(REDOX_SOURCE)
    #define REDOX_EXPORT
#elif defined(_MSC_VER)
    #if defined(REDOX_EXPORTS) || defined(redox_EXPORTS)
        #define REDOX_EXPORT  __declspec(dllexport)
    #else
        #define REDOX_EXPORT  __declspec(dllimport)
    #endif
#elif defined(__GNUC__)
    #define REDOX_EXPORT  __attribute__((visibility("default")))
#else
    #define REDOX_EXPORT
#endif
