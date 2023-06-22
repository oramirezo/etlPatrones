HDFS_PATH="/bdaimss/la/la_aficobranza/ssregp"
LA_PATH="/data/users/oscar_ramirez/aficobranza/ssregp/la"
LT_PATH="/data/users/oscar_ramirez/aficobranza/ssregp/lt"

#LA_PATH="C:/Users/hrodriguez/Documents/imss/testing"
#LT_PATH="C:/Users/hrodriguez/PycharmProjects/etlAfiliacion/la"

cd /

echo "> Removing..."
if rm -r ${LT_PATH}; then
  echo "[ok] ${LT_PATH} is clean now!."
else
  echo "[Continue] ${LT_PATH}..."
fi
cd /
if rm -r ${LA_PATH}/*; then
  echo "[ok] ${LA_PATH} is clean now!."
else
  echo "[Continue] ${LA_PATH}..."
fi

echo "> Creating temp directories..."
if mkdir ${LA_PATH}; then
  echo "[ok] ${LA_PATH} created successfully!."
else
    echo "[error] Creating ${LA_PATH}."
fi

#if mkdir ${LA_PATH}/sut_cifras_sua; then
#  echo "[ok] ${LA_PATH} created successfully!."
#else
#    echo "[error] Creating ${LA_PATH}/sut_cifras_sua."
#fi

echo "> Copying HDFS LA -> Local LA"
if hdfs dfs -get ${HDFS_PATH}/*.txt ${LA_PATH}; then
  echo "[ok] HDFS LA -> Local LA successfully!"
else
  echo "[error] Transferring HDFS LA -> Local LA"
fi
