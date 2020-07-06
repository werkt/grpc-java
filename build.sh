CXXFLAGS=-I$HOME/proto/include LDFLAGS=-L$HOME/proto/lib ./gradlew -PskipAndroid=true build

for mod in api core netty; do cp $mod/build/libs/grpc-$mod-1.28.0.jar ../bf2/bazel-bf2/external/maven/v1/https/repo.maven.apache.org/maven2/io/grpc/grpc-$mod/1.28.0/; done
