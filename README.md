# Clowncar

## Dependencies

### Install [Blosc] (https://github.com/Blosc/c-blosc)

* Untar latest release
* make a build directory
* cmake -DCMAKE\_INSTALL\_PREFIX=your\_install\_prefix\_directory ..
* cmake --build .
* ctest
* ctest --build . --target install

### Build

* export LD\_LIBRARY\_PATH=your\_install\_prefix\_directory/include:your\_install\_prefix\_directory/lib
* mvn clean test

### Package

* export LD\_LIBRARY\_PATH=your\_install\_prefix\_directory/include:your\_install\_prefix\_directory/lib
* mvn clean package
