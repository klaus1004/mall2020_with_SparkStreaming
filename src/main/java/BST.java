/**
 * 二叉搜索树
 ***不可有重复元素
 */
public class BST {
    private Integer rootvalue;
    private BST leftBST;
    private BST rightBST;

    public Integer getRootvalue() {
        return rootvalue;
    }

    public void setRootvalue(Integer rootvalue) {
        this.rootvalue = rootvalue;
    }

    public BST getLeftBST() {
        return leftBST;
    }

    public void setLeftBST(BST leftBST) {
        this.leftBST = leftBST;
    }

    public BST getRightBST() {
        return rightBST;
    }

    public void setRightBST(BST rightBST) {
        this.rightBST = rightBST;
    }

    public BST(int rootvalue){
    this.rootvalue=rootvalue;
    }

//    /**
//     * 传入一个数组生成对应二叉搜索树，存在重复元素时自动跳过重复值
//     * @param array
//     */
//    public BST(int[] array){
//        for (int i=1;i<array.length;i++){
//            this.insert(array[i]);
//        }
//
//    }

    /**
     * 插入一个元素到BST树，存在相同值返回false
     * @param newValue
     * @return
     */
    public boolean insertNode(int newValue){
        if(newValue<this.getRootvalue()){
            if (this.getLeftBST()==null){
                this.setLeftBST(new BST(newValue));
                return true;
            }else{
                this.getLeftBST().insertNode(newValue);
            }
        }else if(newValue>this.getRootvalue()){
            if (this.getRightBST()==null){
                this.setRightBST(new BST(newValue));
                return true;
            }else{
                this.getRightBST().insertNode(newValue);
            }
        }return false;
    }

    /**
     * 判断是否存在某个值
     * @param value
     * @return
     */
    public boolean hasNode(int value){
        if (this.getRootvalue()==value){
            return true;
        }else if(value<this.getRootvalue()){
            if(this.getLeftBST()==null){
                return false;
            }else {
                return this.getLeftBST().hasNode(value);
            }
        }else if(value>this.getRootvalue()){
            if (this.getRightBST()==null){
                return false;
            }else {
                return this.getRightBST().hasNode(value);
            }
        }
        return false;
    }

    /**
     * 先序遍历
     */
    public void firstTraversal(){
        System.out.print(this.getRootvalue()+" ");
        if (this.getLeftBST()!=null) {
            this.getLeftBST().firstTraversal();
        }
        if (this.getRightBST()!=null) {
            this.getRightBST().firstTraversal();
        }
    }

    /**
     * 删除一个节点,返回删除后的结果树
     * @param value
     * @return
     */
    public BST removeNode(int value){

        if (this.getRootvalue()==value){
            if(this.getLeftBST()!=null&&this.getRightBST()!=null){
                BST minNode = this.getRightBST().getMinNode();
                this.setRootvalue(minNode.getRootvalue());
                this.setRightBST(minNode.removeNode(minNode.getRootvalue()));
            }else if(this.getLeftBST()==null&&this.getRightBST()==null){
                return null;
            }else if(this.getLeftBST()==null){
                return this.getRightBST();
            }else {
                return this.getLeftBST();
            }
        }else if(value<this.getRootvalue()){
            if(this.getLeftBST()==null){
                return this;
            }else {
                this.setLeftBST(this.getLeftBST().removeNode(value));
            }
        }else if(value>this.getRootvalue()){
            if (this.getRightBST()==null){
                return this;
            }else {
                this.setRightBST(this.getRightBST().removeNode(value));
            }
        }
        return this;
    }

    /**
     *查询BST树中根值最小的BST子树
     * @return
     */
    public BST getMinNode(){
        if (this.getLeftBST()==null){
            return this;
        }else {
            return this.getLeftBST().getMinNode();
        }
    }
}
